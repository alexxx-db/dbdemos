from .conf import DemoConf, merge_dict
from .exceptions.dbdemos_exception import SDPNotAvailableException, SDPCreationException
from datetime import date


class PipelineInstaller:
    """Creates/updates the SDP (Declarative) pipelines for a demo.

    Extracted from Installer so it can be reused by the JobBundler without
    constructing a full Installer (which triggers Tracker/dbutils side effects).
    Depends only on a DBClient (`db`) and an InstallerReport (`report`).
    """

    def __init__(self, db, report):
        self.db = db
        self.report = report

    def load_demo_pipelines(self, demo_name, demo_conf: DemoConf, debug=False, serverless=False, dlt_policy_id=None, dlt_compute_settings=None):
        #default cluster conf
        pipeline_ids = []
        for pipeline in demo_conf.pipelines:
            definition = pipeline["definition"]
            if "event_log" not in definition:
                definition["event_log"] = {"catalog": demo_conf.catalog, "schema": demo_conf.schema, "name": "dlt_event_log_"}
            if "target" in definition:
                definition["schema"] = definition["target"]
                del definition["target"] #target is deprecated now (https://docs.databricks.com/api/workspace/pipelines/create#schema)
            #Force channel to current due to issue with PREVIEW on serverless with python verison
            definition["channel"] = "CURRENT"
            today = date.today().strftime("%Y-%m-%d")
            #modify cluster definitions if serverless
            if serverless:
                if "clusters" in definition:
                    del definition['clusters']
                definition['photon'] = True
                definition['serverless'] = True
                if dlt_policy_id is not None:
                    self.report.display_pipeline_error(SDPCreationException(f"Policy ID is not supported for serverless pipelines, {dlt_policy_id}", definition, None))
            else:
                #enforce demo tagging in the cluster
                for cluster in definition["clusters"]:
                    merge_dict(cluster, {"custom_tags": {"project": "dbdemos", "demo": demo_name, "demo_install_date": today}})
                    if dlt_policy_id is not None:
                        cluster["dlt_policy_id"] = dlt_policy_id
                    if self.db.conf.get_demo_pool() is not None:
                        cluster["instance_pool_id"] = self.db.conf.get_demo_pool()
                        if "node_type_id" in cluster: del cluster["node_type_id"]
                        if "enable_elastic_disk" in cluster: del cluster["enable_elastic_disk"]
                        if "aws_attributes" in cluster: del cluster["aws_attributes"]
                    if dlt_compute_settings is not None:
                        merge_dict(cluster, dlt_compute_settings)

            existing_pipeline = self.get_pipeline(definition["name"])
            if debug:
                print(f'    Installing pipeline {definition["name"]}')
            if existing_pipeline == None:
                p = self.db.post("2.0/pipelines", definition)
                if 'error_code' in p and p['error_code'] == 'FEATURE_DISABLED':
                    message = f'SDP pipelines are not available in this workspace. Only Premium workspaces are supported on Azure.'
                    pipeline_ids.append({"name": pipeline["definition"]["name"], "uid": "INSTALLATION_ERROR", "id": pipeline["id"], "error": True})
                    self.report.display_pipeline_error(SDPNotAvailableException(message, definition, p))
                    continue
                if 'error_code' in p:
                    pipeline_ids.append({"name": pipeline["definition"]["name"], "uid": "INSTALLATION_ERROR", "id": pipeline["id"], "error": True})
                    self.report.display_pipeline_error(SDPCreationException(f"Error creating the SDP pipeline: {p['error_code']}", definition, p))
                    continue
                id = p['pipeline_id']
            else:
                if debug:
                    print("    Updating existing pipeline with last configuration")
                id = existing_pipeline['pipeline_id']
                p = self.db.put("2.0/pipelines/"+id, definition)
                if 'error_code' in p:
                    pipeline_ids.append({"name": pipeline["definition"]["name"], "uid": "INSTALLATION_ERROR", "id": pipeline["id"], "error": True})
                    if 'complete the migration' in str(p).lower() or 'CANNOT_SET_SCHEMA_FOR_EXISTING_PIPELINE' in str(p):
                        self.report.display_pipeline_error_migration(SDPCreationException(f"Please delete the existing SDP pipeline id {id} before re-installing this demo.", definition, p))
                    else:
                        self.report.display_pipeline_error(SDPCreationException(f"Error updating the SDP pipeline {id}: {p['error_code']}", definition, p))
                    continue
            permissions = self.db.patch(f"2.0/preview/permissions/pipelines/{id}", {
                "access_control_list": [{"group_name": "users", "permission_level": "CAN_MANAGE"}]
            })
            if 'error_code' in permissions:
                print(f"WARN: Couldn't update the pipeline permission for all users to access: {permissions}. Try deleting the pipeline first?")
            pipeline_ids.append({"name": definition['name'], "uid": id, "id": pipeline["id"], "run_after_creation": pipeline["run_after_creation"]})
            #Update the demo conf tags {{}} with the actual id (to be loaded as a job for example)
            demo_conf.set_pipeline_id(pipeline["id"], id)
        return pipeline_ids

    def get_pipeline(self, name):
        def get_pipelines(token=None):
            r = self.db.get("2.0/pipelines", {"max_results": 100, "page_token": token})
            if "statuses" in r:
                for p in r["statuses"]:
                    if p["name"] == name:
                        return p
            if "next_page_token" in r:
                return get_pipelines(r["next_page_token"])
            return None
        return get_pipelines()
