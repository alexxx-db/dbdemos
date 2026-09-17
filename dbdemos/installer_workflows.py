from .conf import DemoConf, merge_dict, ConfTemplate
import json
import time
import pkg_resources

from .exceptions.dbdemos_exception import WorkflowException


def get_resource(path, decode=True):
    resource = pkg_resources.resource_string("dbdemos", path)
    return resource.decode('UTF-8') if decode else resource


def _infer_cloud_from_url(workspace_url):
    """Infer the Databricks cloud from the workspace URL (see Installer._infer_cloud_from_url)."""
    if not workspace_url or workspace_url == "local":
        return "AWS"
    url = workspace_url.lower()
    if "azuredatabricks.net" in url:
        return "AZURE"
    if "gcp.databricks.com" in url:
        return "GCP"
    return "AWS"


class InstallerWorkflow:
    def __init__(self, db, report, get_endpoint_fn=None):
        # Dependency-injected collaborators so this is reusable by JobBundler
        # without constructing a full Installer.
        # get_endpoint_fn(username, demo_conf, warehouse_name=...) -> endpoint dict,
        #   only required for demos whose job definition uses {{SHARED_WAREHOUSE_ID}}.
        self.db = db
        self.report = report
        self.get_endpoint_fn = get_endpoint_fn

    #Start the init job if it exists
    def install_workflows(self, demo_conf: DemoConf, use_cluster_id = None, warehouse_name: str = None, serverless = False, debug = False):
        workflows = []
        if len(demo_conf.workflows) > 0:
            if debug:
                print(f"    Loading demo workflows")
            # We have an init jon
            for workflow in demo_conf.workflows:
                definition = workflow['definition']
                job_name = definition["settings"]["name"]
                # add cloud specific setup
                job_id, run_id = self.create_or_replace_job(demo_conf, definition, job_name, workflow['start_on_install'], use_cluster_id, warehouse_name, serverless, debug)
                # print(f"    Demo workflow available: {self.db.conf.workspace_url}/#job/{job_id}/tasks")
                workflows.append({"uid": job_id, "run_id": run_id, "id": workflow['id']})
        return workflows

    #create or update the init job if it exists
    def create_demo_init_job(self, demo_conf: DemoConf, use_cluster_id = None, warehouse_name: str = None, serverless = False, debug = False, is_bundle = False):
        if "settings" in demo_conf.init_job:
            job_name = demo_conf.init_job["settings"]["name"]
            if debug:
                print(f"    Searching for existing demo initialisation job {job_name}")
            #We have an init json
            job_id, run_id = self.create_or_replace_job(demo_conf, demo_conf.init_job, job_name, False, use_cluster_id, warehouse_name, serverless, debug, is_bundle)
            return {"uid": job_id, "run_id": run_id, "id": "init-job"}
        return {"uid": None, "run_id": None, "id": None}

    #Start the init job if it exists.
    def start_demo_init_job(self, demo_conf: DemoConf, init_job, debug = False):
        if init_job['uid'] is not None:
            j = self.db.post("2.1/jobs/run-now", {"job_id": init_job['uid']})
            if debug:
                print(f'Starting init job {init_job}: {j}')
            if "error_code" in j:
                self.report.display_workflow_error(WorkflowException("Can't start the workflow", {"job_id": init_job['uid']}, init_job, j), demo_conf.name)
            init_job['run_id'] = j['run_id']
            return j['run_id']

    def create_or_replace_job(self, demo_conf: DemoConf, definition: dict,  job_name: str, run_now: bool, use_cluster_id = None, warehouse_name: str = None, serverless = False, debug = False, is_bundle = False):
        cloud = _infer_cloud_from_url(self.db.conf.workspace_url)
        conf_template = ConfTemplate(self.db.conf.username, demo_conf.name)
        cluster_conf = get_resource("resources/default_cluster_job_config.json")
        cluster_conf = json.loads(conf_template.replace_template_key(cluster_conf))
        cluster_conf_cloud = json.loads(get_resource(f"resources/default_cluster_config-{cloud}.json"))
        merge_dict(cluster_conf, cluster_conf_cloud)
        definition = self.replace_warehouse_id(demo_conf, definition, warehouse_name)
        definition = self._apply_bundle_only(definition, is_bundle)
        #Use a given interactive cluster, change the job setting accordingly.
        if use_cluster_id is not None:
            del definition["settings"]["job_clusters"]
            for task in definition["settings"]["tasks"]:
                if "job_cluster_key" in task:
                    del task["job_cluster_key"]
                    task["existing_cluster_id"] = use_cluster_id
        #otherwise set the job properties based on the definition & add pool for our dev workspace.
        else:
            for cluster in definition["settings"]["job_clusters"]:
                if "new_cluster" in cluster:
                    merge_dict(cluster["new_cluster"], cluster_conf, override=False)
                    #Let's make sure we add our dev pool for faster startup
                    if self.db.conf.get_demo_pool() is not None:
                        cluster["new_cluster"]["instance_pool_id"] = self.db.conf.get_demo_pool()
                        cluster["new_cluster"].pop("node_type_id", None)
                        cluster["new_cluster"].pop("enable_elastic_disk", None) 
                        cluster["new_cluster"].pop("aws_attributes", None)

        # Add support for clsuter specific task
        for task in definition["settings"]["tasks"]:
            if "new_cluster" in task:
                merge_dict(task["new_cluster"], cluster_conf, override=False)

        # if we're installing from a serverless cluster, update the job to be fully serverless
        if serverless:
            # Serverless notebook tasks run against an "environment" (client version). We pin the
            # client to env_version (default 5) so notebooks can use env-v5 features like `%uv pip`.
            # A shared default environment covers tasks with no extra libraries; tasks that declare
            # libraries get their own env carrying those pypi dependencies (still on env_version).
            client_version = str(getattr(self.db.conf, "env_version", 5))
            environments = []
            default_env_key = "env_default"
            uses_default_env = False
            for task in definition["settings"]["tasks"]:
                task.pop("new_cluster", None)
                task.pop("job_cluster_key", None)
                task.pop("existing_cluster_id", None)

                # pipeline_task (and other non-notebook tasks) run on their own compute -> no environment.
                if "notebook_task" not in task:
                    task.pop("libraries", None)
                    task.pop("gpu", None)
                    continue

                # GPU serverless task: a task can request a serverless GPU via {"gpu": "GPU_1xA10"}
                # in its bundle_config. A job-level GPU accelerator REQUIRES a job-level base
                # environment (can't rely on the notebook env), and base_environment=databricks_ai_v*
                # is not accepted via the Jobs API, so we pin environment_version="4" and set
                # task.compute.hardware_accelerator. Extra libs must be installed with %pip in the
                # notebook (GPU env dependencies aren't honored the same way).
                gpu = task.pop("gpu", None)
                if gpu:
                    task["compute"] = {"hardware_accelerator": gpu}
                    env_key = "env_gpu_" + task["task_key"]
                    environments.append({
                        "environment_key": env_key,
                        "spec": {"environment_version": "4"}
                    })
                    task["environment_key"] = env_key
                    task.pop("libraries", None)
                    continue

                # Serverless doesn't support libraries. Instead, they have environments and we link
                # these env to each task. Extract libraries if they exist and convert to an environment.
                dependencies = []
                if "libraries" in task:
                    for lib in task["libraries"]:
                        if "pypi" in lib and "package" in lib["pypi"]:
                            dependencies.append(lib["pypi"]["package"])
                    task.pop("libraries", None)

                if dependencies:
                    env_key = "env_" + task["task_key"]
                    environments.append({
                        "environment_key": env_key,
                        "spec": {"client": client_version, "dependencies": dependencies}
                    })
                    task["environment_key"] = env_key
                else:
                    # No extra libs: attach the shared default environment so the task still runs on
                    # the pinned client version (otherwise it falls back to notebook env version 1).
                    task["environment_key"] = default_env_key
                    uses_default_env = True

            if uses_default_env:
                environments.append({"environment_key": default_env_key, "spec": {"client": client_version}})

            definition["settings"].pop("job_clusters", None)
            if environments:
                definition["settings"]["environments"] = environments
        
        existing_job = self.db.find_job(job_name)
        if existing_job is not None:
            job_id = existing_job["job_id"]
            self.db.post("/2.1/jobs/runs/cancel-all", {"job_id": job_id})
            self.wait_for_run_completion(job_id, debug=debug)
            if debug:
                print("    Updating existing job")
            job_config = {"job_id": job_id, "new_settings": definition["settings"]}
            r = self.db.post("2.1/jobs/reset", job_config)
            if "error_code" in r:
                self.report.display_workflow_error(WorkflowException("Can't update the workflow",
                                                                               f"error resetting the workflow, do you have permission?.", job_config, r), demo_conf.name)
        else:
            if debug:
                print("    Creating a new job for demo initialization (data & table setup).")
            r_jobs = self.db.post("2.1/jobs/create", definition["settings"])
            if "error_code" in r_jobs:
                self.report.display_workflow_error(WorkflowException("Can't create the workflow", {}, definition["settings"], r_jobs), demo_conf.name)
            job_id = r_jobs["job_id"]
        if run_now:
            j = self.db.post("2.1/jobs/run-now", {"job_id": job_id})
            if "error_code" in j:
                self.report.display_workflow_error(WorkflowException("Can't start the workflow", {"job_id": job_id}, j), demo_conf.name)
            return job_id, j['run_id']
        return job_id, None

    def _apply_bundle_only(self, definition, is_bundle):
        """Handle tasks tagged with the custom "bundle_only" marker.

        - Always strip the "bundle_only" key before the Jobs API call (unknown field).
        - On a real install (is_bundle=False) drop the bundle_only tasks entirely.
          They are expected to be leaf tasks; we defensively also strip any
          depends_on entry that points at a removed task.
        """
        tasks = definition.get("settings", {}).get("tasks", [])
        if not tasks:
            return definition
        removed_keys = set()
        if not is_bundle:
            kept = []
            for task in tasks:
                if task.get("bundle_only") is True:
                    removed_keys.add(task.get("task_key"))
                else:
                    kept.append(task)
            tasks = kept
        for task in tasks:
            task.pop("bundle_only", None)
            if removed_keys and "depends_on" in task:
                task["depends_on"] = [d for d in task["depends_on"] if d.get("task_key") not in removed_keys]
        definition["settings"]["tasks"] = tasks
        return definition

    def replace_warehouse_id(self, demo_conf: DemoConf, definition, warehouse_name: str = None):
        # Jobs need a warehouse ID. Let's replace it with the one created. TODO: should be in the template?
        if "{{SHARED_WAREHOUSE_ID}}" in json.dumps(definition):
            if self.get_endpoint_fn is None:
                raise WorkflowException(
                    "This job needs a SQL warehouse ({{SHARED_WAREHOUSE_ID}}) but no endpoint provider was configured "
                    "(get_endpoint_fn is None). This demo's init_job can't be run from the bundler context.",
                    {}, definition, None)
            endpoint = self.get_endpoint_fn(self.db.conf.name, demo_conf, warehouse_name = warehouse_name)
            if endpoint is None:
                print(
                    "ERROR: couldn't create or get a SQL endpoint for dbdemos. Do you have permission? Your workflow won't be able to execute the task.")
                #TODO: quick & dirty, need to improve
                definition = json.loads(json.dumps(definition).replace(""", "warehouse_id": "{{SHARED_WAREHOUSE_ID}}"}""", ""))
            else:
                definition = json.loads(json.dumps(definition).replace("{{SHARED_WAREHOUSE_ID}}", endpoint['warehouse_id']))
        return definition

    def wait_for_run_completion(self, job_id, max_retry=10, debug = False):
        def is_still_running(job_id):
            runs = self.db.get("2.1/jobs/runs/list", {"job_id": job_id, "active_only": "true"})
            return "runs" in runs and len(runs["runs"]) > 0
        i = 0
        while i <= max_retry and is_still_running(job_id):
            if debug:
                print(f"      A run is still running for job {job_id}, waiting for termination...")
            time.sleep(5)