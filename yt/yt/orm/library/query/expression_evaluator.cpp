#include "expression_evaluator.h"
#include "query_evaluator.h"
#include "query_rewriter.h"

#include <yt/yt/orm/library/attributes/attribute_path.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/string/cast.h>

namespace NYT::NOrm::NQuery {
namespace {

using namespace NQueryClient::NAst;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

using NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("ExpressionEvaluator");

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributeType(EValueType valueType)
{
    switch (valueType) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean:
        case EValueType::String:
        case EValueType::Any:
            return;
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::Max:
            THROW_ERROR_EXCEPTION("Attribute type %Qlv is not supported", valueType);
        default:
            YT_ABORT();
    }
}

void ValidateAttributePaths(const std::vector<TTypedAttributePath>& typedAttributePaths)
{
    if (typedAttributePaths.empty()) {
        THROW_ERROR_EXCEPTION("At least one attribute path must be provided");
    }

    for (const auto& typedPath : typedAttributePaths) {
        ValidateAttributeType(typedPath.Type);
        NAttributes::ValidateAttributePath(typedPath.Path);
    }

    for (size_t i = 0; i < typedAttributePaths.size(); ++i) {
        for (size_t j = 0; j < typedAttributePaths.size(); ++j) {
            if (i == j) {
                continue;
            }
            if (NYPath::HasPrefix(typedAttributePaths[i].Path, typedAttributePaths[j].Path)) {
                THROW_ERROR_EXCEPTION(
                    "Attribute paths must be independent, but %Qv is a prefix of %Qv",
                    typedAttributePaths[i].Path,
                    typedAttributePaths[j].Path);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue MakeUnversionedValue(
    EValueType valueType,
    const TNonOwningAttributePayload& payload,
    TObjectsHolder* objectsHolder)
{
    if (valueType == EValueType::String) {
        return Visit(
            payload,
            [&] (const TYsonStringBuf& ysonPayload) {
                // Currently, it's impossible to create a non-owning string view from TYsonString.
                // Since TUnversionedValue itself is non-owning, an external holder is needed.
                auto* regularString = objectsHolder->New<TString>();
                *regularString = NYson::ConvertFromYsonString<TString>(ysonPayload);
                return MakeUnversionedStringValue(*regularString);
            },
            [&] (const TStringBuf& stringPayload) {
                return MakeUnversionedStringValue(stringPayload);
            });
    }

    const auto& ysonPayload = std::invoke([&] () -> const auto& {
        if (const auto* ysonPayloadPtr = std::get_if<TYsonStringBuf>(&payload)) {
            return *ysonPayloadPtr;
        }
        THROW_ERROR_EXCEPTION("Values of type %Qlv can be parsed from yson payload only",
            valueType);
    });

    switch (valueType) {
        case EValueType::Int64: {
            return MakeUnversionedInt64Value(NYson::ConvertFromYsonString<i64>(ysonPayload));
        }
        case EValueType::Uint64: {
            return MakeUnversionedUint64Value(NYson::ConvertFromYsonString<ui64>(ysonPayload));
        }
        case EValueType::Double: {
            return MakeUnversionedDoubleValue(NYson::ConvertFromYsonString<double>(ysonPayload));
        }
        case EValueType::Boolean: {
            return MakeUnversionedBooleanValue(NYson::ConvertFromYsonString<bool>(ysonPayload));
        }
        case EValueType::Any: {
            if (ysonPayload == TYsonStringBuf("#")) {
                return MakeUnversionedNullValue();
            }
            return MakeUnversionedAnyValue(ysonPayload.AsStringBuf());
        }
        default:
            // Attribute type validation should have been performed earlier.
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TExpressionEvaluator
    : public IExpressionEvaluator
{
public:
    TExpressionEvaluator(TString query, std::vector<TTypedAttributePath> typedAttributePaths)
        : Query_(std::move(query))
        , TypedAttributePaths_(std::invoke([&] {
            ValidateAttributePaths(typedAttributePaths);

            return std::move(typedAttributePaths);
        }))
        , EvaluationContext_(CreateQueryEvaluationContext(
            CreateFakeTableFilterExpression(),
            CreateFakeTableSchema()))
    { }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        TRowBufferPtr rowBuffer) override
    {
        try {
            if (!rowBuffer) {
                rowBuffer = New<TRowBuffer>(TRowBufferTag());
            }
            if (attributePayloads.size() != TypedAttributePaths_.size()) {
                THROW_ERROR_EXCEPTION("Invalid number of attributes: expected %v, but got %v",
                    TypedAttributePaths_.size(),
                    attributePayloads.size());
            }

            TObjectsHolder temporaryObjectsHolder;
            std::vector<TUnversionedValue> inputValues;
            inputValues.reserve(attributePayloads.size());
            for (size_t index = 0; index < TypedAttributePaths_.size(); ++index) {
                inputValues.push_back(MakeUnversionedValue(
                    TypedAttributePaths_[index].Type,
                    attributePayloads[index],
                    &temporaryObjectsHolder));
            }

            return EvaluateQuery(
                *EvaluationContext_,
                MakeRange(inputValues.data(), inputValues.size()),
                rowBuffer);
        } catch (const std::exception& ex) {
            return TError("Error evaluating query")
                << TErrorAttribute("query", Query_)
                << ex;
        }
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const TNonOwningAttributePayload& attributePayload,
        TRowBufferPtr rowBuffer) override
    {
        return Evaluate(
            std::vector<TNonOwningAttributePayload>{attributePayload},
            std::move(rowBuffer));
    }

private:
    TObjectsHolder ObjectsHolder_;

    const TString Query_;
    const std::vector<TTypedAttributePath> TypedAttributePaths_;
    const std::unique_ptr<TQueryEvaluationContext> EvaluationContext_;

    struct TRowBufferTag
    { };

    const TString& GetFakeTableColumnName(const NYPath::TYPath& attributePath) const
    {
        static const TString Default = "data";
        return attributePath.empty()
            ? Default
            : attributePath;
    }

    TReferenceExpressionPtr CreateFakeTableColumnReference(const NYPath::TYPath& attributePath)
    {
        return ObjectsHolder_.New<TReferenceExpression>(
            NQueryClient::NullSourceLocation,
            GetFakeTableColumnName(attributePath));
    }

    TTypedAttributePath GetMatchingAttributePath(const TYPath& queryAttributePath)
    {
        for (const auto& dataAttributePath : TypedAttributePaths_) {
            if (NYPath::HasPrefix(queryAttributePath, dataAttributePath.Path)) {
                return dataAttributePath;
            }
        }
        THROW_ERROR_EXCEPTION("Attribute path %Qv refers to a forbidden attribute",
            queryAttributePath);
    }

    TExpressionPtr CreateFakeTableAttributeSelector(const TYPath& queryAttributePath)
    {
        try {
            auto typedDataAttributePath = GetMatchingAttributePath(queryAttributePath);
            const auto& dataAttributePath = typedDataAttributePath.Path;
            auto queryAttributePathSuffix = queryAttributePath.substr(dataAttributePath.size());

            if (queryAttributePathSuffix.Empty()) {
                return CreateFakeTableColumnReference(dataAttributePath);
            }
            if (typedDataAttributePath.Type != EValueType::Any) {
                THROW_ERROR_EXCEPTION(
                    "Attribute path of type %Qlv does not support nested attributes",
                    typedDataAttributePath.Type);
            }

            return ObjectsHolder_.New<TFunctionExpression>(
                NQueryClient::NullSourceLocation,
                "try_get_any",
                TExpressionList{
                    CreateFakeTableColumnReference(dataAttributePath),
                    ObjectsHolder_.New<TLiteralExpression>(
                        NQueryClient::NullSourceLocation,
                        std::move(queryAttributePathSuffix))});
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating query selector for attribute path %Qv",
                queryAttributePath)
                << ex;
        }
    }

    NQueryClient::NAst::TExpressionPtr CreateFakeTableFilterExpression()
    {
        auto parsedQuery = ParseSource(Query_, NQueryClient::EParseMode::Expression);
        auto queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

        ObjectsHolder_.Merge(std::move(parsedQuery->AstHead));

        auto referenceMapping = [&] (const TReference& reference) {
            if (reference.TableName) {
                THROW_ERROR_EXCEPTION("Table references are not supported");
            }
            return CreateFakeTableAttributeSelector(reference.ColumnName);
        };
        TQueryRewriter rewriter(std::move(referenceMapping));

        return rewriter.Run(queryExpression);
    }

    TTableSchemaPtr CreateFakeTableSchema()
    {
        std::vector<TColumnSchema> columns;
        for (const auto& typedPath : TypedAttributePaths_) {
            columns.emplace_back(
                GetFakeTableColumnName(typedPath.Path),
                typedPath.Type);
        }
        return New<TTableSchema>(std::move(columns));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IExpressionEvaluatorPtr CreateExpressionEvaluator(
    TString query,
    std::vector<TTypedAttributePath> typedAttributePaths)
{
    return New<TExpressionEvaluator>(
        std::move(query),
        std::move(typedAttributePaths));
}

IExpressionEvaluatorPtr CreateExpressionEvaluator(
    TString query,
    std::vector<TString> attributePaths)
{
    std::vector<TTypedAttributePath> typedAttributePaths;
    typedAttributePaths.reserve(attributePaths.size());

    for (auto& path : attributePaths) {
        typedAttributePaths.push_back(TTypedAttributePath{
            .Path = std::move(path),
            .Type = EValueType::Any,
        });
    }
    return New<TExpressionEvaluator>(
        std::move(query),
        std::move(typedAttributePaths));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
