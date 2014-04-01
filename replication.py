from checks import AgentCheck


class Replication(AgentCheck):
    def __init__(self, name, init_config, agentConfig):
        AgentCheck.__init__(self, name, init_config, agentConfig)
        self.dbs = {}

    def check(self, instance):
        host = instance.get('host', '')
        port = instance.get('port', '')
        user = instance.get('username', '')
        password = instance.get('password', '')
        tags = instance.get('tags', [])
        dbname = instance.get('database', 'postgres')

        # Clean up tags in case there was a None entry in the instance
        # e.g. if the yaml contains tags: but no actual tags
        if tags is None:
            tags = []
        key = '%s:%s' % (host, port)

        db = self.get_connection(key, host, port, user, password, dbname)

        # Collect metrics
	query = "SELECT extract( epoch from now() - pg_last_xact_replay_timestamp() ) AS delay;"
        cursor = db.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]

        self.gauge('mp.postgresql.replication.delay', unicode(result) )
	
    def get_connection(self, key, host, port, user, password, dbname):

        if key in self.dbs:
            return self.dbs[key]

        elif host != '' and user != '':
            try:
                import psycopg2 as pg
            except ImportError:
                raise ImportError("psycopg2 library can not be imported. Please check the installation instruction on the Datadog Website")

            if host == 'localhost' and password == '':
                # Use ident method
                connection = pg.connect("user=%s dbname=%s" % (user, dbname))
            elif port != '':
                connection = pg.connect(host=host, port=port, user=user,
                    password=password, database=dbname)
            else:
                connection = pg.connect(host=host, user=user, password=password,
                    database=dbname)
        connection.set_isolation_level(0)

        self.dbs[key] = connection
        return connection

    @staticmethod
    def parse_agent_config(agentConfig):
        server = agentConfig.get('postgresql_server','')
        port = agentConfig.get('postgresql_port','')
        user = agentConfig.get('postgresql_user','')
        passwd = agentConfig.get('postgresql_pass','')

        if server != '' and user != '':
            return {
                'instances': [{
                    'host': server,
                    'port': port,
                    'username': user,
                    'password': passwd
                }]
            }

        return False

