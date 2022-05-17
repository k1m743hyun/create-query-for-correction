import re
import tqdm
import datetime
import psycopg2


# DB 접속 정보
HOST = ''
PORT = 0
DBNAME = ''
USER = ''
PASSWORD = ''


# Query Pattern
QUERY_PATTERN_DICT = {
    "insert": "INSERT INTO (?P<table>[^\(]*)(?P<fields>.*) VALUES (?P<values>.*) (?P<option>ON CONFLICT DO NOTHING)",
    "update": "UPDATE (?P<table>.*) SET (?P<values>.*) WHERE (?P<condition>.*)",
    "delete": "DELETE FROM (?P<table>.*) WHERE (?P<condition>.*)"
}


def getPostgresQuery(host, port, dbname, user, password, query):
    # DB 접속
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()

    # Query 실행
    cur.execute(query)

    rows = None
    if query.split(' ')[0] == 'SELECT':
        rows = cur.fetchall()
    else:
        conn.commit()

    # DB 접속 종료
    cur.close()
    conn.close()

    return rows


def main():
    # Primary Key 찾는 Query
    query_for_searching_pk = """
    SELECT T.table_name, T.pk_column, S.query_num, S.origin_sql
    FROM tmp_correction_sql AS S
        JOIN tmp_table_meta AS T
            ON T.table_num = S.table_num
    WHERE is_analyzed = 'N'
    ORDER BY S.query_num DESC;
    """
    
    for table_name, pk_column, query_num, origin_sql in tqdm.tqdm(getPostgresQuery(HOST, PORT, DBNAME, USER, PASSWORD, query_for_searching_pk)):
        # SQL 문 전처리
        origin_sql = re.sub('Error occurred while processing sync request. SQL Redo: ', '', origin_sql)
        origin_sql = re.sub('\n', ' ', origin_sql)

        # SQL 문 명령어 추출
        command = origin_sql.split(' ')[0].lower()
        
        update_dict = {}
        try:
            if command == 'insert':
                # SQL 문 파싱
                data_dict = re.compile(QUERY_PATTERN_DICT[command], re.I).match(origin_sql).groupdict()

                # Value 뽑아서 List에 적재
                field_list = list(map(lambda x: x.strip(), data_dict['fields'].strip()[1:-1].split(',')))
                value_list = list(map(lambda x: x.strip(), data_dict['values'].strip()[1:-1].split(',')))

                # pk_value 생성
                pk_column_value = ','.join(map(lambda x: x.replace("'", ""), [value_list[field_list.index(k)] for k in field_list for key in pk_column.split(',') if key and re.compile(key, re.I).match(k)]))

                # 보정 쿼리 생성
                correct_sql = origin_sql.replace('ON CONFLICT', 'ON CONFLICT ({})'.format(pk_column)).replace('NOTHING', 'UPDATE SET ') + ','.join(["{}={}".format(k, v) for k, v in zip(field_list, value_list)])

                # return 값
                update_dict = {
                    "pk_column_value": pk_column_value,
                    "is_analyzed": "Y",
                    "analyzed_date": str(datetime.datetime.now()),
                    "is_correct_target": "i",
                    "correct_sql": correct_sql.replace("'", "''"),
                    "correct_target_date": str(datetime.datetime.now())
                }

            elif command == 'update':
                # SQL 문 파싱
                data_dict = re.compile(QUERY_PATTERN_DICT[command], re.I).match(origin_sql).groupdict()

                # Value 뽑아서 Dict에 적재
                condition_dict = {k.split('=')[0]: k.split('=')[1] for k in [c for c in data_dict['condition'].split(' and ')]}
                values_dict = {k.split('=')[0]: k.split('=')[1] for k in [c for c in data_dict['values'].split(',')]}

                # WHERE 문에는 있지만 SET 문에는 없는 키 찾아 추가
                for k in (set(condition_dict) - set(values_dict)):
                    values_dict[k] = condition_dict[k]

                # pk_dict 추출
                pk_dict = {k: v for key in pk_column.split(',') for k, v in values_dict.items() if key and re.compile(key, re.I).match(k)}

                # pk_value 생성
                pk_column_value = ','.join(map(lambda x: x.replace("'", ""), pk_dict.values()))

                # UPDATE 문 생성
                #correct_sql = re.compile('UPDATE (?P<table>.*) SET ').match(origin_sql).group() + ','.join(['{}={}'.format(k, v) for k, v in values_dict.items()]) + ' WHERE ' + ' and '.join(['{}={}'.format(k, v) for k, v in pk_dict.items()])

                # 보정 쿼리 생성 => INSERT 문으로 변경
                correct_sql = 'INSERT INTO ' + data_dict['table'] + '(' + ','.join(values_dict.keys()) + ') VALUES (' + ','.join(values_dict.values()) + ') ON CONFLICT DO NOTHING'

                # return 값
                update_dict = {
                    "pk_column_value": pk_column_value,
                    "is_analyzed": "Y",
                    "analyzed_date": str(datetime.datetime.now()),
                    "is_correct_target": "u",
                    "correct_sql": correct_sql.replace("'", "''"),
                    "correct_target_date": str(datetime.datetime.now())
                }

            # DELETE WHERE에 PK만 추출
            elif command == 'delete':
                # SQL 문 파싱
                data_dict = re.compile(QUERY_PATTERN_DICT[command], re.I).match(origin_sql).groupdict()

                # Value 뽑아서 Dict에 적재
                condition_dict = {k.split('=')[0]: k.split('=')[1] for k in [c for c in data_dict['condition'].split(' and ')]}

                # pk_dict 추출
                pk_dict = {k: v for key in pk_column.split(',') for k, v in condition_dict.items() if key and re.compile(key, re.I).match(k)}

                # pk_value 생성
                pk_column_value = ','.join(map(lambda x: x.replace("'", ""), pk_dict.values()))

                # DELETE 문 생성
                correct_sql = re.compile('DELETE FROM (?P<table>.*) WHERE ').match(origin_sql).group() + ' and '.join(['{}={}'.format(k, v) for k, v in pk_dict.items()])

                # return 값
                update_dict = {
                    "pk_column_value": pk_column_value,
                    "is_analyzed": "Y",
                    "analyzed_date": str(datetime.datetime.now()),
                    "is_correct_target": "d",
                    "correct_sql": correct_sql.replace("'", "''"),
                    "correct_target_date": str(datetime.datetime.now())
                }

            else:
                pass

        except Exception as e:
            print('[ERROR] {}'.format(e))

        # Table에 적재
        query_for_correction = "UPDATE tmp_correction_sql SET {values} WHERE {condition}".format(values=','.join(["{}='{}'".format(k, str(v)) for k, v in update_dict.items()]), condition='query_num={}'.format(query_num))
        getPostgresQuery(HOST, PORT, DBNAME, USER, PASSWORD, query_for_correction)


if __name__ == '__main__':
    main()
