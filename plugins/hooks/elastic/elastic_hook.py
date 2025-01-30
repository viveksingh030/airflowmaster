from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):

    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id)
        
        # Parse connection extra parameters
        extra = conn.extra_dejson  # Airflow's built-in JSON parser
        scheme = extra.get("scheme", "http")  # Default to HTTP if not specified

        hosts = []
        if conn.host:
            for host_entry in conn.host.split(','):
                host_entry = host_entry.strip()
                if ':' in host_entry:
                    host, port = host_entry.split(':', 1)
                    port = int(port.strip())
                else:
                    host = host_entry.strip()
                    port = int(conn.port) if conn.port else 9200
                
                hosts.append({
                    'host': host,
                    'port': port,
                    'scheme': scheme  # <-- Add scheme here
                })

        conn_config = {}
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        # Add SSL verification if using HTTPS
        if scheme == "https":
            conn_config.update({
                'use_ssl': True,
                'verify_certs': extra.get("verify_certs", False),
                'ca_certs': extra.get("ca_certs")
            })

        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema
    
    def info(self):
        return self.es.info()

    def set_index(self, index):
        self.index = index

    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res
    
class ElasticAirflowPlugin(AirflowPlugin):
    name='elastic'
    hooks=[ElasticHook]

    def on_load(*args, **kwargs):
        # perform Plugin boot actions
        pass