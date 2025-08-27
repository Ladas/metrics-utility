import datetime

import polars as pd

from django.db import connection


class ExtractorControllerDB:
    def __init__(self, extra_params):
        super().__init__()

        self.extra_params = extra_params

    def iter_batches(self):
        with connection.cursor() as cursor:
            cursor.execute(self.pg_functions())

            since = self.extra_params['opt_since']
            if since.tzinfo is None:
                since = since.replace(tzinfo=datetime.UTC)

            marker_cond = ''
            while True:
                cursor.execute(self.host_metric_query(since, marker_cond))
                host_metric = self.dict_fetchall(cursor)

                # Marker based pagination
                if len(host_metric) <= 0:
                    break

                marker_cond = f"""
                    AND CONCAT(main_hostmetric.hostname , '___', COALESCE(main_host.id, 0)) >
                        '{list(host_metric)[-1]['hostname']}___{list(host_metric)[-1]['host_id']}'
                    -- TODO wrong query, has to use several or conditions, but will it help with usage of index?
                    -- AND main_hostmetric.hostname >= '{list(host_metric)[-1]['hostname']}'
                    -- AND COALESCE(main_host.id, 0) > {list(host_metric)[-1]['host_id']}
                """

                host_metric = pd.DataFrame(host_metric)

                yield {'host_metric': host_metric}

    def dict_fetchall(self, cursor):
        """
        Return all rows from a cursor as a dict.
        Assume the column names are unique.
        """
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def pg_functions(self):
        query = """
            -- Define function for parsing field out of yaml encoded as text
            CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field(
                str text,
                field text
            )
            RETURNS text AS
            $$
            DECLARE
                line_re text;
                field_re text;
            BEGIN
                field_re := ' *[:=] *(.+?) *$';
                line_re := '(?n)^' || field || field_re;
                RETURN trim(both '"' from substring(str from line_re) );
            END;
            $$
            LANGUAGE plpgsql;

            -- Define function to check if field is a valid json
            CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json(p_json text)
                returns boolean
            AS
            $$
            BEGIN
                RETURN (p_json::json is not null);
            EXCEPTION
                WHEN others THEN
                    RETURN false;
            END;
            $$
            LANGUAGE plpgsql;
        """
        return query

    def host_metric_query(self, since, marker_cond=''):
        query = f"""
            SELECT main_hostmetric.hostname,
                   COALESCE(main_host.id, 0) AS host_id,
                   main_hostmetric.first_automation,
                   main_hostmetric.last_automation,
                   main_hostmetric.automated_counter,
                   main_hostmetric.deleted_counter,
                   main_hostmetric.last_deleted,
                   main_hostmetric.deleted,
                   main_host.ansible_facts->>'ansible_product_serial'::TEXT AS ansible_product_serial,
                   main_host.ansible_facts->>'ansible_machine_id'::TEXT AS ansible_machine_id,
                   CASE
                       WHEN (metrics_utility_is_valid_json(main_host.variables))
                           THEN main_host.variables::jsonb->>'ansible_host'
                       ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                   END AS ansible_host_variable,
                   CASE
                       WHEN (metrics_utility_is_valid_json(main_host.variables))
                           THEN main_host.variables::jsonb->>'ansible_connection'
                       ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                   END AS ansible_connection_variable

            FROM main_hostmetric
            LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname
            WHERE (main_hostmetric.last_automation >= '{since.isoformat()}' {marker_cond})
            ORDER BY CONCAT(main_hostmetric.hostname , '___', COALESCE(main_host.id, 0)) ASC
            -- ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
            LIMIT {self.limit()}
        """

        return query

    @staticmethod
    def limit():
        return 10000
