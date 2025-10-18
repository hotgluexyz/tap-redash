import logging
import json
import sys
from typing import Any, Dict, List, Optional
import requests as req
import singer
from time import sleep
import io
import csv
import backoff

logger = singer.get_logger()
logger.setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

REQUIRED_CONFIG_KEYS = ['BASE_URL', 'API_KEY']
args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

class RetriableException(Exception):
    """Exception for retriable errors."""
    pass


class Redash:

    def __init__(self) -> None:
        try:
            self._config: Dict[str, Any] = args.config
        except Exception as e:
            raise IOError(e)

        self._session = req.Session()
        self._timeout = (10, 60)  # (connect, read) seconds
        self._base_url = self._config['BASE_URL'].rstrip('/')
        self._api_key = self._config['API_KEY']
        
        self.query_id_filter: Optional[str] = self._config.get('QUERY_ID')

    # -------- Fetch Available Queries -------- #

    def _get_available_queries(self) -> List[Dict[str, Any]]:
        """Fetch list of all available queries from Redash."""
        url = f"{self._base_url}/api/queries"
        params = {'api_key': self._api_key}
        
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            payload = resp.json()
        except req.RequestException as e:
            logger.critical("Error fetching Redash queries list: %s", e)
            raise
        except ValueError as e:
            logger.critical("Invalid JSON from Redash queries endpoint: %s", e)
            raise

        try:
            # Redash returns {"results": [...], "count": N, ...}
            queries = payload.get('results', [])
            if not isinstance(queries, list):
                raise TypeError("Redash queries payload is not a list.")
        except (KeyError, TypeError) as e:
            logger.critical("Unexpected Redash queries payload shape: %s", e)
            raise

        return queries

    def get_queries_for_catalog(self) -> List[Dict[str, Any]]:
        """
        Get queries that should be included in the catalog.
        If QUERY_ID is specified, return only that query; otherwise return all.
        """
        if self.query_id_filter:
            # Fetch single query metadata
            query_id = str(self.query_id_filter)
            url = f"{self._base_url}/api/queries/{query_id}"
            params = {'api_key': self._api_key}
            
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                resp.raise_for_status()
                query = resp.json()
                return [query]
            except req.RequestException as e:
                logger.warning("Error fetching query %s: %s", query_id, e)
                return []
        else:
            # Fetch all queries
            return self._get_available_queries()
    
    def initialize_query(self, query_id: str, offset, limit) -> str:
        """Initialize a query."""
        url = f"{self._base_url}/api/queries/{query_id}/results"
        params = {'api_key': self._api_key}
        payload = {"max_age": 0, "parameters": {"offset": offset, "limit": limit}}
        resp = self._session.post(url, params=params, json=payload, timeout=self._timeout)
        try:
            resp.raise_for_status()
        except req.RequestException as e:
            logger.warning("Error initializing query %s: %s. Response: %s", query_id, e, resp.text)
            return None
        return resp.json().get('job', {}).get('id')

    def get_query_result_id(self, job_id: str) -> int:
        """Check the status of a job and return the query result id if the job is complete."""
        url = f"{self._base_url}/api/jobs/{job_id}"
        params = {'api_key': self._api_key}
        max_wait_time = 300  # 5 minutes in seconds
        total_sleep_time = 0
        sleep_time = 5

        while total_sleep_time < max_wait_time:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            try:
                resp.raise_for_status()
                # if status is 3 it means the job is complete and we can return the query result id
                if resp.json().get('job', {}).get('status') in [3]:
                    return resp.json().get('job', {}).get('query_result_id')
                
                # if status is 4 or 5 it means the job failed or was cancelled log and return None to skip the query
                elif resp.json().get('job', {}).get('status') in [4, 5]:
                    logger.warning("Job %s failed to complete with status: %s. Skipping.", job_id, resp.json().get('job', {}).get('status'))
                    return None
                
                logger.warning("Job %s is still running. Sleeping for %s seconds and checking again.", job_id, sleep_time)
                sleep(sleep_time)
                total_sleep_time += sleep_time
            except req.RequestException as e:
                logger.warning("Error checking job status %s: %s. Response: %s", job_id, e, resp.text)
                break
    
    def validate_response(self, resp: req.Response) -> None:
        """Validate the response from the API."""
        if 500 <= resp.status_code < 600:
            raise RetriableException(f"Error: {resp.status_code} {resp.text}")
        resp.raise_for_status()


    @backoff.on_exception(
        backoff.expo,
        (RetriableException),
        max_tries=5,
        factor=2,
        base=2
    )
    def get_query_results(self, query_result_id: str, discover: bool = False, stream_name: str = None) -> List[Dict[str, Any]]:
        """Get the data for a query result."""
        url = f"{self._base_url}/api/query_results/{query_result_id}.csv"
        params = {'api_key': self._api_key}
        resp = self._session.get(url, params=params, timeout=self._timeout)
        try:
            self.validate_response(resp)
            if discover:
                # For discovery, only return first 100 records for schema inference
                with io.StringIO(resp.text) as csv_data:
                    reader = csv.DictReader(csv_data)
                    records = []
                    for i, row in enumerate(reader):
                        if i >= 100:
                            break
                        # Clean empty strings to None
                        cleaned_row = {k: (None if v == '' else v) for k, v in row.items()}
                        records.append(cleaned_row)
                    return records
            # For sync, read CSV in chunks and stream to output
            with io.StringIO(resp.text) as csv_data:

                # Get the full content
                data = csv_data.getvalue()

                # Remove NUL characters
                cleaned = data.replace('\x00', '')

                csv_data = io.StringIO(cleaned)
                reader = csv.DictReader(csv_data)
                for row in reader:
                    # Clean empty strings to None
                    cleaned_row = {k: (None if v == '' else v) for k, v in row.items()}
                    singer.write_record(stream_name, cleaned_row)

            return data.count('\n') >= 2
               
        except req.RequestException as e:
            logger.warning("Error getting query result data %s: %s. Response: %s", query_result_id, e, resp.text)
            raise

    # -------- Fetch Query Data -------- #

    def _get_query_data(self, query_id: str, discover: bool = False, stream_name: str = None) -> List[Dict[str, Any]]:
        """Fetch the results for a specific query."""
        url = f"{self._base_url}/api/queries/{query_id}/results.json"
        params = {'api_key': self._api_key}
        
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            payload = resp.json()
        except req.RequestException as e:
            logger.warning("Error fetching query %s results: %s", query_id, e)
            if resp.json().get('message') == 'No cached result found for this query.':
                offset = 0
                limit = 10_000
                while True:
                    job_id = self.initialize_query(query_id, offset, limit)
                    if job_id is None:
                        logger.warning("Failed to initialize query %s. Skipping.", query_id)
                        return None
                    query_result_id = self.get_query_result_id(job_id)
                    if query_result_id:
                        rows = self.get_query_results(query_result_id, discover, stream_name)
                        offset += limit
                        if discover:
                            return rows
                        if not rows:
                            break
                    else:
                        logger.warning("Job failed to complete for query '%s'. Skipping.", query_id,)
                        return None
            return []
        except ValueError as e:
            logger.warning("Invalid JSON from query %s results: %s", query_id, e)
            return []

        try:
            rows = payload['query_result']['data']['rows']
            if not isinstance(rows, list):
                logger.warning("Query %s rows payload is not a list.", query_id)
                return []
        except (KeyError, TypeError) as e:
            logger.warning("Unexpected payload shape for query %s: %s", query_id, e)
            return []

        return rows

    # -------- Schema Inference -------- #

    @staticmethod
    def _singer_type_for_value(value: Any) -> Dict[str, Any]:
        """Infer JSON Schema from a single Python value (no merging)."""
        if value is None:
            return {"type": "null"}
        if isinstance(value, bool):
            return {"type": "boolean"}
        if isinstance(value, int):
            return {"type": "integer"}
        if isinstance(value, float):
            return {"type": "number"}
        if isinstance(value, str):
            return {"type": "string"}
        if isinstance(value, dict):
            return {
                "type": "object",
                "properties": {
                    k: Redash._singer_type_for_value(v)
                    for k, v in value.items()
                },
                "additionalProperties": False
            }
        if isinstance(value, list):
            if not value:
                return {"type": "array"}
            
            if value[0] is None:
                items_schema = {"type": "array", "items": {"type": "string"}} # if list is empty, default items type to strings
            else:
                items_schema = Redash._singer_type_for_value(value[0])
            
            return {
                "type": "array",
                "items": items_schema
            }
        return {"type": "string"}

    @staticmethod
    def _merge_schemas(schemas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple schemas into one with combined types, always nullable."""
        merged: Dict[str, Any] = {}

        # collect all types
        types = []
        for s in schemas:
            t = s.get("type")
            if isinstance(t, list):
                types.extend(t)
            else:
                types.append(t)

        # always allow null
        types.append("null")

        if len(set(types)) == 1:
            types.append("string")

        merged["type"] = sorted(set(types))

        # merge object properties if needed
        if "object" in types:
            merged["properties"] = {}
            for s in schemas:
                if "properties" in s:
                    for k, v in s["properties"].items():
                        if k not in merged["properties"]:
                            merged["properties"][k] = v
                        else:
                            merged["properties"][k] = Redash._merge_schemas(
                                [merged["properties"][k], v]
                            )
            merged["additionalProperties"] = False

        # merge array items if needed
        if "array" in types:
            item_schemas = [s.get("items") for s in schemas if "items" in s]
            if item_schemas:
                merged["items"] = Redash._merge_schemas(item_schemas)

        return merged

    def _infer_properties(self, sample_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Infer schema properties from sample rows."""
        MAX_SCAN = min(100, len(sample_rows))
        union: Dict[str, set] = {}

        for i in range(MAX_SCAN):
            row = sample_rows[i]
            if not isinstance(row, dict):
                continue
            for k, v in row.items():
                schema = Redash._singer_type_for_value(v)
                union.setdefault(k, []).append(schema)

        return {
            k: Redash._merge_schemas(schemas)
            for k, schemas in union.items()
        }

    def generate_stream_entry(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a stream entry for the Singer catalog from a query object."""
        query_id = str(query['id'])
        query_name = query.get('name', f'query_{query_id}')
        
        # Sanitize stream name (remove special chars, use underscores)
        stream_name = query_name.replace(' ', '_').replace('-', '_')
        stream_name = ''.join(c for c in stream_name if c.isalnum() or c == '_')
        stream_name = stream_name.lower()
        
        # If stream name is empty after sanitization, use query_id
        if not stream_name:
            stream_name = f'query_{query_id}'

        # Fetch sample data to infer schema
        logger.info("Fetching sample data for query %s: %s", query_id, query_name)
        data = self._get_query_data(query_id, discover=True)
        
        if not data:
            properties: Dict[str, Any] = {}
            logger.warning("No data for query %s, using empty schema", query_id)
            return None
        else:
            properties = self._infer_properties(data)

        key_props = self._config.get("key_properties", [])
        if not isinstance(key_props, list):
            key_props = []
        
        metadata = []
        # Add breadcrumb entries for each property
        for prop_name in properties.keys():
            metadata.append({
                "breadcrumb": ["properties", prop_name],
                "metadata": {
                    "inclusion": "available"
                }
            })
        
        metadata.append({
            "breadcrumb": [],
            "metadata": {
                "query_id": query_id,
                "query_name": query_name,
                "selected": True  # Auto-select all streams by default
            }
        })

        stream_entry: Dict[str, Any] = {
            "stream": stream_name,
            "tap_stream_id": query_id,
            "schema": {
                "type": "object",
                "properties": properties,
                "additionalProperties": False,
            },
            "key_properties": key_props,
            "metadata": metadata
        }
        return stream_entry

    # -------- Singer IO -------- #

    def do_discover(self) -> Dict[str, Any]:
        """Discovery mode: generate catalog with all available queries as streams."""
        queries = self.get_queries_for_catalog()
        
        if not queries:
            logger.warning("No queries found in Redash instance")
            catalog = {"streams": []}
        else:
            logger.info("Found %d queries to include in catalog", len(queries))
            streams = []
            for query in queries:
                if query.get('id') not in self._config.get('query_ids', []):
                    continue
                try:
                    stream_entry = self.generate_stream_entry(query)
                    if stream_entry is None:
                        continue
                    streams.append(stream_entry)
                except Exception as e:
                    query_id = query.get('id', 'unknown')
                    logger.warning("Failed to generate stream for query %s: %s", query_id, e)
                    continue
            
            catalog = {"streams": streams}

        print(json.dumps(catalog, indent=2))
        return catalog

    def output_to_stream(self, catalog: Dict[str, Any]) -> None:
        """Emit schema and records for selected streams."""
        if not catalog or "streams" not in catalog:
            logger.critical("Invalid catalog; no streams found.")
            return

        for stream in catalog["streams"]:
            # Check if stream is selected
            selected = True
            if "metadata" in stream:
                for metadata_entry in stream["metadata"]:
                    if not metadata_entry.get("breadcrumb"):  # Root level metadata
                        selected = metadata_entry.get("metadata", {}).get("selected", True)

            if not selected:
                logger.info("Skipping unselected stream: %s", stream["stream"])
                continue

            stream_name = stream["stream"]
            tap_stream_id = stream.get("tap_stream_id", stream_name)
            schema = stream["schema"]
            key_props = stream.get("key_properties", [])

            logger.info("Syncing stream: %s (query_id: %s)", stream_name, tap_stream_id)

            # Write schema
            singer.write_schema(stream_name, schema, key_props)

            # Fetch and write records
            data = self._get_query_data(tap_stream_id, stream_name=stream_name)
            if data:
                singer.write_records(stream_name, data)
                logger.info("Wrote %d records for stream %s", len(data), stream_name)
            else:
                logger.warning("No records found for stream %s", stream_name)


def main() -> None:
    rdash = Redash()

    if args.discover:
        rdash.do_discover()
        return

    # In sync mode, use provided catalog or generate one
    if args.properties:
        catalog = args.properties
    else:
        logger.info("No catalog provided, generating from available queries")
        catalog = rdash.do_discover()

    if not isinstance(catalog, dict) or "streams" not in catalog:
        logger.critical("Invalid catalog format")
        sys.exit(1)

    rdash.output_to_stream(catalog)
    sys.stdout.flush()


if __name__ == "__main__":
    main()