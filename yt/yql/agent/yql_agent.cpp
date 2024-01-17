#include "yql_agent.h"

#include "config.h"
#include "interop.h"

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yql/plugin/bridge/plugin.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NYTree;
using namespace NHiveClient;
using namespace NYqlClient;
using namespace NYqlClient::NProto;
using namespace NYson;
using namespace NHiveClient;
using namespace NSecurityClient;
using namespace NLogging;

const auto& Logger = YqlAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TYqlAgent
    : public IYqlAgent
{
public:
    TYqlAgent(
        TSingletonsConfigPtr singletonsConfig,
        TYqlAgentConfigPtr yqlAgentConfig,
        TClusterDirectoryPtr clusterDirectory,
        TClientDirectoryPtr clientDirectory,
        IInvokerPtr controlInvoker,
        TString agentId)
        : SingletonsConfig_(std::move(singletonsConfig))
        , Config_(std::move(yqlAgentConfig))
        , ClusterDirectory_(std::move(clusterDirectory))
        , ClientDirectory_(std::move(clientDirectory))
        , ControlInvoker_(std::move(controlInvoker))
        , AgentId_(std::move(agentId))
        , ThreadPool_(CreateThreadPool(Config_->YqlThreadCount, "Yql"))
    {
        static const TYsonString EmptyMap = TYsonString(TString("{}"));

        auto clustersConfig = Config_->GatewayConfig->AsMap()->GetChildOrThrow("cluster_mapping")->AsList();

        auto singletonsConfigString = SingletonsConfig_
            ? ConvertToYsonString(*SingletonsConfig_)
            : EmptyMap;

        THashSet<TString> presentClusters;
        for (const auto& cluster : clustersConfig->GetChildren()) {
            presentClusters.insert(cluster->AsMap()->GetChildOrThrow("name")->GetValue<TString>());
        }

        for (const auto& clusterName : ClusterDirectory_->GetClusterNames()) {
            if (presentClusters.contains(clusterName)) {
                continue;
            }

            auto cluster = NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("name").Value(clusterName)
                    .Item("cluster").Value(clusterName)
                .EndMap();
            auto settings = TYqlPluginConfig::MergeClusterDefaultSettings(GetEphemeralNodeFactory()->CreateList());
            cluster->AsMap()->AddChild("settings", std::move(settings));
            clustersConfig->AddChild(std::move(cluster));
        }

        NYqlPlugin::TYqlPluginOptions options{
            .SingletonsConfig = singletonsConfigString,
            .GatewayConfig = ConvertToYsonString(Config_->GatewayConfig),
            .FileStorageConfig = ConvertToYsonString(Config_->FileStorageConfig),
            .OperationAttributes = ConvertToYsonString(Config_->OperationAttributes),
            .YTTokenPath = Config_->YTTokenPath,
            .LogBackend = NYT::NLogging::CreateArcadiaLogBackend(TLogger("YqlPlugin")),
            .YqlPluginSharedLibrary = Config_->YqlPluginSharedLibrary,
        };
        YqlPlugin_ = NYqlPlugin::CreateYqlPlugin(std::move(options));
    }

    void Start() override
    { }

    void Stop() override
    { }

    NYTree::IMapNodePtr GetOrchidNode() const override
    {
        return GetEphemeralNodeFactory()->CreateMap();
    }

    void OnDynamicConfigChanged(
        const TYqlAgentDynamicConfigPtr& /*oldConfig*/,
        const TYqlAgentDynamicConfigPtr& /*newConfig*/) override
    { }

    TFuture<std::pair<TRspStartQuery, std::vector<TSharedRef>>> StartQuery(TQueryId queryId, const TString& impersonationUser, const TReqStartQuery& request) override
    {
        YT_LOG_INFO("Starting query (QueryId: %v, ImpersonationUser: %v)", queryId, impersonationUser);

        return BIND(&TYqlAgent::DoStartQuery, MakeStrong(this), queryId, impersonationUser, request)
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    TRspGetQueryProgress GetQueryProgress(TQueryId queryId) override
    {
        YT_LOG_DEBUG("Getting query progress (QueryId: %v)", queryId);

        TRspGetQueryProgress response;

        YT_LOG_DEBUG("Getting progress from YQL plugin");

        try {
            auto result = YqlPlugin_->GetProgress(queryId);
            if (result.YsonError) {
                auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                THROW_ERROR error;
            }
            YT_LOG_DEBUG("YQL plugin progress call completed");
            YT_LOG_DEBUG("PROGRESS. progress: %Qv", result.Progress.value_or("no progress"));
            YT_LOG_DEBUG("PROGRESS. plan: %Qv", result.Plan.value_or("no plan"));
            YT_LOG_DEBUG("PROGRESS. YsonResult: %Qv", result.YsonResult.value_or("no YsonResult"));
            YT_LOG_DEBUG("PROGRESS. Statistics: %Qv", result.Statistics.value_or("no Statistics"));
            YT_LOG_DEBUG("PROGRESS. TaskInfo: %Qv", result.TaskInfo.value_or("no TaskInfo"));

            if (result.Plan || result.Progress) {
            YT_LOG_DEBUG("PROGRESS If1");
                TYqlResponse yqlResponse;
            YT_LOG_DEBUG("PROGRESS If2");
                ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
            YT_LOG_DEBUG("PROGRESS If3");
                ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
            YT_LOG_DEBUG("PROGRESS If4");
                response.mutable_yql_response()->Swap(&yqlResponse);
            YT_LOG_DEBUG("PROGRESS If5");
            }
            YT_LOG_DEBUG("PROGRESS Returning response");
            return response;
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_DEBUG("PROGRESS YQL plugin call failed");
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

private:
    const TSingletonsConfigPtr SingletonsConfig_;
    const TYqlAgentConfigPtr Config_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr ControlInvoker_;
    const TString AgentId_;

    std::unique_ptr<NYqlPlugin::IYqlPlugin> YqlPlugin_;

    IThreadPoolPtr ThreadPool_;

    std::pair<TRspStartQuery, std::vector<TSharedRef>> DoStartQuery(TQueryId queryId, const TString& impersonationUser, const TReqStartQuery& request)
    {
        static const auto EmptyMap = TYsonString(TString("{}"));

        const auto& Logger = YqlAgentLogger.WithTag("QueryId: %v", queryId);

        const auto& yqlRequest = request.yql_request();

        TRspStartQuery response;

        YT_LOG_INFO("Invoking YQL embedded");
        YT_LOG_DEBUG("YQL AGENT RECEIVED REQUEST: %Qv", yqlRequest.DebugString());

        std::vector<TSharedRef> wireRowsets;
        try {
            YT_LOG_DEBUG("START 1");
            auto query = Format("pragma yt.UseNativeYtTypes; pragma ResultRowsLimit=\"%v\";\n%v", request.row_count_limit(), yqlRequest.query());
            YT_LOG_DEBUG("START 2");
            auto settings = yqlRequest.has_settings() ? TYsonString(yqlRequest.settings()) : EmptyMap;

            std::vector<NYqlPlugin::TQueryFile> files;
            files.reserve(yqlRequest.files_size());
            for (const auto& file : yqlRequest.files()) {
                files.push_back(NYqlPlugin::TQueryFile{
                    .Name = file.name(),
                    .Content = file.content(),
                    .Type = static_cast<EQueryFileContentType>(file.type()),
                });
            }

            // This is a long blocking call.
            YT_LOG_DEBUG("START 3");
            auto result = YqlPlugin_->Run(queryId, impersonationUser, query, settings, files);
            YT_LOG_DEBUG("START progress: %Qv", result.Progress.value_or("no progress"));
            YT_LOG_DEBUG("START plan: %Qv", result.Plan.value_or("no plan"));
            YT_LOG_DEBUG("START YsonResult: %Qv", result.YsonResult.value_or("no YsonResult"));
            YT_LOG_DEBUG("START Statistics: %Qv", result.Statistics.value_or("no Statistics"));
            YT_LOG_DEBUG("START TaskInfo: %Qv", result.TaskInfo.value_or("no TaskInfo"));
            YT_LOG_DEBUG("START YsonError: %Qv", result.YsonError.value_or("no YsonError"));
            if (result.YsonError) {
                YT_LOG_DEBUG("START 4");
            auto error = ConvertTo<TError>(TYsonString(*result.YsonError));
                YT_LOG_DEBUG("START 5");
            THROW_ERROR error;
            }

            YT_LOG_INFO("YQL plugin call completed");

            TYqlResponse yqlResponse;
            YT_LOG_DEBUG("START 6");
            ValidateAndFillYqlResponseField(yqlResponse, result.YsonResult, &TYqlResponse::mutable_result);
            YT_LOG_DEBUG("START 7");
            ValidateAndFillYqlResponseField(yqlResponse, result.Plan, &TYqlResponse::mutable_plan);
            YT_LOG_DEBUG("START 8");
            ValidateAndFillYqlResponseField(yqlResponse, result.Statistics, &TYqlResponse::mutable_statistics);
            YT_LOG_DEBUG("START 9");
            ValidateAndFillYqlResponseField(yqlResponse, result.Progress, &TYqlResponse::mutable_progress);
            YT_LOG_DEBUG("START 10");
            ValidateAndFillYqlResponseField(yqlResponse, result.TaskInfo, &TYqlResponse::mutable_task_info);
            YT_LOG_DEBUG("START 11");
            if (request.build_rowsets() && result.YsonResult) {
                YT_LOG_DEBUG("START IF1");
                auto rowsets = BuildRowsets(ClientDirectory_, *result.YsonResult, request.row_count_limit());

                YT_LOG_DEBUG("START IF2");
                for (const auto& rowset : rowsets) {
                    if (rowset.Error.IsOK()) {
                        wireRowsets.push_back(rowset.WireRowset);
                        response.add_rowset_errors();
                        response.add_incomplete(rowset.Incomplete);
                    } else {
                        wireRowsets.push_back(TSharedRef());
                        ToProto(response.add_rowset_errors(), rowset.Error);
                        response.add_incomplete(false);
                    }
                }
            }
            YT_LOG_DEBUG("START 6");
            response.mutable_yql_response()->Swap(&yqlResponse);
            YT_LOG_DEBUG("START 7");
            return {response, wireRowsets};
        } catch (const std::exception& ex) {
            auto error = TError("YQL plugin call failed") << TError(ex);
            YT_LOG_DEBUG("START YQL plugin call failed");
            YT_LOG_INFO(error, "YQL plugin call failed");
            THROW_ERROR error;
        }
    }

    void ValidateAndFillYqlResponseField(TYqlResponse& yqlResponse, const std::optional<TString>& rawField, TString* (TYqlResponse::*mutableProtoFieldAccessor)())
    {
        if (!rawField) {
            return;
        }
        // TODO(max42): original YSON tends to unnecessary pretty.
        *((&yqlResponse)->*mutableProtoFieldAccessor)() = *rawField;
    };
};

IYqlAgentPtr CreateYqlAgent(
    TSingletonsConfigPtr singletonsConfig,
    TYqlAgentConfigPtr config,
    TClusterDirectoryPtr clusterDirectory,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr controlInvoker,
    TString agentId)
{
    return New<TYqlAgent>(
        std::move(singletonsConfig),
        std::move(config),
        std::move(clusterDirectory),
        std::move(clientDirectory),
        std::move(controlInvoker),
        std::move(agentId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
