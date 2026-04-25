 
from datetime import datetime

from pydantic.dataclasses import dataclass

from analyse_file import FileDispatcher, UnsupportedFileTypeError, get_file_metadata_from_shared_drive
from automic_apis import get_job_logs
from configuration import AutomicConfig
from database_util import add_ms_access_table, read_ms_db_by_query
from log_parser import parse_job_log, read_log_from_shared_drive
import ast
from abc import ABC, abstractmethod
import json
from pathlib import Path


@dataclass
class RuleContext:
    base_url: str
    config: AutomicConfig
    db_path: str
    rule: dict




class BaseRule(ABC):
    def __init__(self, context: RuleContext):
        self.ctx = context
        self.rule = context.rule

    @abstractmethod
    def execute(self):
        pass

    def save_result(self, result):
        data = {
            "job_id": self.rule["job_id"],
            "job_run_id": self.rule["run_id"],
            "workflow_run_id": self.rule["workflow_run_id"],
            "rule_id": self.rule["rule_id"],
            "result": result,
            "last_update_time": datetime.now()
        }
        add_ms_access_table(
            self.ctx.db_path,
            table_name="rules_result",
            data=data
        )
 
class FileTransferStatsRule(BaseRule):

    def execute(self):
        log_resp = get_job_logs(
            base_url=self.ctx.base_url,
            config=self.ctx.config,
            run_id=self.rule["run_id"]
        )

        result = parse_job_log(
            log_resp,
            external_log_loader=read_log_from_shared_drive
        )

        self.save_result(str(result))




class FileMetadataRule_old( ):

    def process_data(self,params):
        file_path = params["file_path"]
        operation = params["operation"]
        ref_rule_id = params["ref_rule_id"]

        query = """
            SELECT result FROM rules_result
            WHERE job_id = ? AND workflow_run_id = ? AND rule_id = ?
        """

        db_result = read_ms_db_by_query(
            self.ctx.db_path,
            query,
            ["result"],
            [
                self.rule["job_id"],
                self.rule["workflow_run_id"],
                ref_rule_id
            ]
        )

        output = "no file to get metadata"

        if db_result:
            job_result = db_result[0]["result"]
            if job_result != "[]":
                previous_result = ast.literal_eval(job_result)
                metadata = []

                for item in previous_result:
                    if item["operation"] == operation:
                        for f in item.get("files", []):
                            full_path = f"{file_path}{f}"
                            metadata.append(
                                get_file_metadata_from_shared_drive(full_path)
                            )

                output = metadata
        return output
    
    def execute(self):
        params = ast.literal_eval(self.rule["rule_param"])
        output = ""
        if type(params) is dict:
            output = str(self.process_data(params))
        elif  type(params) is list:
            ls_out = []
            for param in params :
                ls_out.extend(self.process_data(param)  )
            output = str(ls_out)
        self.save_result(output)




class FileMetadataRule(BaseRule):

    def process_data(self, params):
        file_path = params["file_path"]
        operation = params["operation"]
        ref_rule_id = params["ref_rule_id"]

        query = """
            SELECT result FROM rules_result
            WHERE job_id = ? AND workflow_run_id = ? AND rule_id = ?
        """

        db_result = read_ms_db_by_query(
            self.ctx.db_path,
            query,
            ["result"],
            [
                self.rule["job_id"],
                self.rule["workflow_run_id"],
                ref_rule_id
            ]
        )

        if not db_result or db_result[0]["result"] == "[]":
            return []

        # previous_result = json.loads(db_result[0]["result"])
        previous_result = ast.literal_eval(db_result[0]["result"])
        metadata = []

        for item in previous_result:
            if item.get("operation") != operation:
                continue

            for f in item.get("files", []):
                full_path = str(Path(file_path) / f)
                try:
                    metadata.append(
                        get_file_metadata_from_shared_drive(full_path)
                    )
                except Exception as e:
                    self.ctx.logger.error(
                        f"Failed to get metadata for {full_path}: {e}"
                    )

        return metadata

    def execute(self):
        # params = json.loads(self.rule["rule_param"])
        rule_param = self.rule["rule_param"]
        params = ast.literal_eval(rule_param)
        results = []

        if isinstance(params, dict):
            results = self.process_data(params)
        elif isinstance(params, list):
            for param in params:
                results.extend(self.process_data(param))

        self.save_result(json.dumps(results))

class GetRecordCountRule_old():

    def execute(self):
        params = ast.literal_eval(self.rule["rule_param"])

        file_path = params["file_path"]
        operation = params["operation"]
        file_type = params.get("file_type",None)

        query = """
            SELECT result FROM rules_result
            WHERE job_id = ? AND workflow_run_id = ? AND rule_id = ?
        """

        db_result = read_ms_db_by_query(
            self.ctx.db_path,
            query,
            ["result"],
            [self.rule["job_id"], self.rule["workflow_run_id"], 1]
        )

        dispatcher = FileDispatcher()
        grouped_count = {}
        file_counts = []

        if db_result:
            job_result = db_result[0]["result"]

            if job_result != "[]":
                results = ast.literal_eval(job_result)

                for res in results:
                    if res["operation"] == operation:
                        for file_name in res.get("files", []):
                            try:
                                full_path = f"{file_path}{file_name}"
                                count = dispatcher.dispatch(
                                    file_type=file_type,
                                    file_name=file_name,
                                    full_file_path=full_path
                                )

                                file_counts.append({file_name: count})
                                grouped_count[res["file_pattern"]] = (
                                    grouped_count.get(res["file_pattern"], 0) + count
                                )

                            except UnsupportedFileTypeError:
                                continue
                            except Exception:
                                continue

        output = str({
            "grouped_count": grouped_count,
            "file_count": file_counts
        })

        self.save_result(output)




class GetRecordCountRule(BaseRule):

    def execute(self):
        params = ast.literal_eval(self.rule["rule_param"])

        # Normalize params → always a list
        if isinstance(params, dict):
            params_list = [params]
        elif isinstance(params, list):
            params_list = params
        else:
            raise ValueError("rule_param must be dict or list of dicts")

        query = """
            SELECT result FROM rules_result
            WHERE job_id = ? AND workflow_run_id = ? AND rule_id = ?
        """

        db_result = read_ms_db_by_query(
            self.ctx.db_path,
            query,
            ["result"],
            [
                self.rule["job_id"],
                self.rule["workflow_run_id"],
                1
            ]
        )

        dispatcher = FileDispatcher()
        grouped_count = {}
        file_counts = []

        if not db_result or db_result[0]["result"] == "[]":
            self.save_result(str({
                "grouped_count": grouped_count,
                "file_count": file_counts
            }))
            return

        results = ast.literal_eval(db_result[0]["result"])

        # Process each param independently
        for param in params_list:
            file_path = param["file_path"]
            operation = param["operation"]
            file_type = param.get("file_type")

            for res in results:
                if res.get("operation") != operation:
                    continue

                for file_name in res.get("files", []):
                    try:
                        full_path = str(Path(file_path) / file_name)

                        count = dispatcher.dispatch(
                            file_type=file_type,
                            file_name=file_name,
                            full_file_path=full_path
                        )

                        file_counts.append({file_name: count})

                        pattern = res.get("file_pattern")
                        if pattern:
                            grouped_count[pattern] = (
                                grouped_count.get(pattern, 0) + count
                            )

                    except UnsupportedFileTypeError:
                        continue
                    except Exception:
                        continue

        output = {
            "grouped_count": grouped_count,
            "file_count": file_counts
        }

        self.save_result(str(output))

class RuleRegistry:
    _rules = {}

    @classmethod
    def register(cls, rule_name: str, rule_cls):
        cls._rules[rule_name] = rule_cls

    @classmethod
    def get(cls, rule_name: str):
        return cls._rules.get(rule_name)
    

RuleRegistry.register("FILE_TRANSFER_STATS", FileTransferStatsRule)
RuleRegistry.register("FILE_METADATA", FileMetadataRule)
RuleRegistry.register("GET_RECORD_COUNT", GetRecordCountRule)

class RuleEngine:

    def __init__(self, base_url, config, db_path):
        self.base_url = base_url
        self.config = config
        self.db_path = db_path

    def run(self, rows):
        for rule in rows:
            try:
                ctx = RuleContext(
                    base_url=self.base_url,
                    config=self.config,
                    db_path=self.db_path,
                    rule=rule
                )

                rule_name = rule["rule_name"]
                rule_cls = RuleRegistry.get(rule_name)

                if not rule_cls:
                    raise ValueError(f"Unknown rule: {rule_name}")

                rule_instance = rule_cls(ctx)
                rule_instance.execute()

            except Exception as ex:
                print(
                    f"Error processing workflow_run_id={rule.get('workflow_run_id')} "
                    f"job_run_id={rule.get('run_id')} => {ex}"
                )
                break