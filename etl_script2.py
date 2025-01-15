#!/usr/bin/env python3
# etl_script.py

import os
import sys
import logging
import pandas as pd
import mysql.connector
import jaydebeapi
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv
from mysql.connector import errorcode
import re

# .env 파일 로드 (보안을 위해 환경 변수 사용 권장)
load_dotenv()

# 로그 설정
today = datetime.now().strftime("%Y%m%d")
log_dir = os.getenv('LOG_DIR', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f'etl_{today}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# MySQL 연결 설정 함수
def get_db_connection():
    try:
        db = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', '175.196.7.45'),
            user=os.getenv('MYSQL_USER', 'nolboo'),
            password=os.getenv('MYSQL_PASSWORD', '2024!puser'),
            database=os.getenv('MYSQL_DATABASE', 'nolboo'),
            charset='utf8mb4'
        )
        logging.info("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
        return db
    except mysql.connector.Error as err:
        logging.error(f"MySQL 연결 오류: {err}")
        return None

# Informix 연결 설정
informix_jdbc_driver_class = 'com.informix.jdbc.IfxDriver'
informix_hostname = os.getenv('INFORMIX_HOST', '175.196.7.17')
informix_port = os.getenv('INFORMIX_PORT', '1526')
informix_database = os.getenv('INFORMIX_DATABASE', 'nolbooco')
informix_server = os.getenv('INFORMIX_SERVER', 'nbmain')
informix_username = os.getenv('INFORMIX_USERNAME', 'informix')
informix_password = os.getenv('INFORMIX_PASSWORD', 'eusr2206')  # 보안을 위해 환경 변수 사용 권장
jdbc_driver_path = os.getenv('JDBC_DRIVER_PATH', '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar')

# Informix JDBC URL 생성 (로케일 설정 유지)
informix_jdbc_url = (
    f"jdbc:informix-sqli://{informix_hostname}:{informix_port}/{informix_database}:"
    f"INFORMIXSERVER={informix_server};DBLOCALE=en_US.819;CLIENT_LOCALE=en_us.utf8;"
)

# 프로그램 설정
OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# 데이터 변환 함수 (작동 잘되는 함수 그대로 유지)
def convert_to_utf8(value):
    if isinstance(value, str):
        try:
            temp_byte = value.encode('ISO-8859-1')  # 원본 인코딩에 맞게 수정 필요
            utf8_value = temp_byte.decode('euc-kr')  # Informix 데이터가 EUC-KR 인코딩이라면
            return utf8_value
        except Exception as e:
            logging.error(f"Failed to decode value '{value}': {e}")
            return value  # 디코딩 실패 시 원본 값 반환
    return value

def check_special_characters(df, columns):
    pattern = re.compile(r'[^\x00-\x7F]+')
    for col in columns:
        if col in df.columns:
            problematic_rows = df[df[col].apply(lambda x: bool(pattern.search(x)) if isinstance(x, str) else False)]
            if not problematic_rows.empty:
                logging.warning(f"컬럼 '{col}'에 특수 문자가 포함된 데이터가 존재합니다.")
                logging.info(problematic_rows[[col]].to_string(index=False))
        else:
            logging.warning(f"'{col}' 컬럼이 데이터프레임에 존재하지 않습니다.")

def extract_data(conn, query, params=None):
    try:
        cursor = conn.cursor()
        logging.info(f"실행할 쿼리: {query} with params: {params}")
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        logging.info(f"데이터 추출 완료. 총 {len(df)}개의 레코드.")
        cursor.close()
        return df
    except Exception as e:
        logging.error(f"데이터 추출 오류: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)

def save_to_excel(df, path):
    try:
        df.to_excel(path, index=False)
        logging.info(f"데이터 엑셀로 저장 완료: {path}")
    except Exception as e:
        logging.error(f"엑셀 파일 저장 오류: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)

def insert_mysql_ARTransactionsLedger(conn, df):
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO ARTransactionsLedger (
                transaction_date,
                representative_code,
                client,
                outlet_name,
                debit,
                food_material_sales
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = [
            (
                row['date'],
                ' ',
                row['client'],
                row['outlet_name'],
                row['debit'],
                row['food_material_sales']
            )
            for index, row in df.iterrows()
        ]
        cursor.executemany(insert_query, data)
        conn.commit()
        logging.info(f"MySQL ARTransactionsLedger 테이블에 {cursor.rowcount}개의 레코드 삽입 완료.")
        cursor.close()
    except mysql.connector.Error as err:
        logging.error(f"MySQL ARTransactionsLedger 삽입 오류: {err}")
        conn.rollback()
        sys.exit(1)

def insert_mysql_AROrderDetailsItem(conn, df):
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO AROrderDetailsItem (
                order_date,
                rep_code,
                rep_name,
                client_code,
                client_name,
                item_code,
                item_name,
                unit,
                qty,
                unit_price,
                order_amount,
                vat,
                total_amount,
                tax
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (
                row['order_date'],
                row['rep_code'] if 'rep_code' in row else None,
                row['rep_name'] if 'rep_name' in row else None,
                row['client_code'] if 'client_code' in row else None,
                row['client_name'] if 'client_name' in row else None,
                row['item_code'] if 'item_code' in row else None,
                row['item_name'] if 'item_name' in row else None,
                row['unit'] if 'unit' in row else None,
                row['qty'] if 'qty' in row else None,
                row['unit_price'] if 'unit_price' in row else None,
                row['order_amount'] if 'order_amount' in row else None,
                row['vat'] if 'vat' in row else None,
                row['total_amount'] if 'total_amount' in row else None,
                row['tax'] if 'tax' in row else None
            )
            for index, row in df.iterrows()
        ]
        cursor.executemany(insert_query, data)
        conn.commit()
        logging.info(f"MySQL AROrderDetailsItem 테이블에 {cursor.rowcount}개의 레코드 삽입 완료.")
        cursor.close()
    except mysql.connector.Error as err:
        logging.error(f"MySQL AROrderDetailsItem 삽입 오류: {err}")
        conn.rollback()
        sys.exit(1)

def insert_mysql_AROrderDetails(conn, df):
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO AROrderDetails (
                representative_code,
                client_code,
                client_name,
                collector_key,
                manager,
                order_date,
                order_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (
                ' ',
                row['client_code'],
                row['client_name'],
                ' ',
                ' ',
                row['order_date'],
                row['order_amount']
            )
            for index, row in df.iterrows()
        ]
        cursor.executemany(insert_query, data)
        conn.commit()
        logging.info(f"MySQL AROrderDetails 테이블에 {cursor.rowcount}개의 레코드 삽입 완료.")
        cursor.close()
    except mysql.connector.Error as err:
        logging.error(f"MySQL AROrderDetails 삽입 오류: {err}")
        conn.rollback()
        sys.exit(1)

# Helper function to convert order_date values in AROrderDetailsItem
# (Since AROrderDetails.order_date is correct, we want to use that new date.)
def use_new_order_date(fallback_date):
    # Simply return the fallback date for every row.
    return fallback_date

def etl_process():
    try:
        logging.info("ETL 프로세스 시작.")

        # 처리할 날짜 입력 (명령줄 인수 또는 프롬프트)
        try:
            if len(sys.argv) > 1:
                input_date_str = sys.argv[1]
                logging.info(f"명령줄 인수로 받은 날짜: {input_date_str}")
            else:
                input_date_str = input("처리할 날짜를 입력하세요 (YYYY-MM-DD): ")
                logging.info(f"프롬프트로 받은 날짜: {input_date_str}")
        except Exception as e:
            logging.error(f"날짜 입력 오류: {e}")
            logging.error(traceback.format_exc())
            sys.exit(1)

        # 날짜 유효성 검사 및 형식 변환
        try:
            input_date = datetime.strptime(input_date_str, "%Y-%m-%d").date()
            # Calculate new date as input_date + 1 day
            new_date = input_date + timedelta(days=1)
            formatted_input_date_str = input_date.strftime('%Y%m%d')  # e.g. '20241227'
            formatted_new_date_str = new_date.strftime('%Y-%m-%d')      # e.g. '2024-12-28'
            now = datetime.now()
            timestamp = now.strftime("%H%M%S")
            logging.info(f"원래 날짜: {formatted_input_date_str}, 업데이트될 날짜 (입력+1): {formatted_new_date_str}, 타임스탬프: {timestamp}")
        except ValueError:
            logging.error("잘못된 날짜 형식입니다. YYYY-MM-DD 형식으로 입력해주세요.")
            sys.exit(1)

        # Informix 데이터베이스 연결
        try:
            conn = jaydebeapi.connect(
                informix_jdbc_driver_class,
                informix_jdbc_url,
                [informix_username, informix_password],
                jdbc_driver_path
            )
            logging.info("Informix 데이터베이스에 성공적으로 연결되었습니다.")
        except Exception as e:
            logging.error(f"Informix 연결 오류: {e}")
            logging.error(traceback.format_exc())
            sys.exit(1)

        # Step 3 데이터 추출 쿼리 (정상 쿼리 참조)
        query_step3 = f"""
            SELECT date, 
                   chain_no,
                   full_name, 
                   rechain_no, 
                   rep_full_name, 
                   item_no, 
                   item_full_name, 
                   unit,
                   qty, 
                   time, 
                   remark, 
                   out_date, 
                   item_price, 
                   item_tax, 
                   tax,
                   (qty * (item_price + item_tax)) AS total
            FROM (
                SELECT a.date AS date, 
                       b.chain_no AS chain_no,
                       b.full_name AS full_name, 
                       b.rechain_no AS rechain_no, 
                       c.full_name AS rep_full_name, 
                       a.item_no AS item_no, 
                       d.full_name AS item_full_name, 
                       d.unit,
                       a.qty AS qty, 
                       a.time AS time, 
                       a.remark AS remark, 
                       a.out_date AS out_date, 
                       CASE 
                           WHEN b.contract_no = '2' THEN 
                               CASE 
                                   WHEN d.PACKAGE_MODEL_PRICE = 0 THEN d.MODEL_PRICE 
                                   ELSE d.PACKAGE_MODEL_PRICE 
                               END 
                           ELSE 
                               CASE 
                                   WHEN d.PACKAGE_CHAIN_PRICE = 0 THEN d.CHAIN_PRICE 
                                   ELSE d.PACKAGE_CHAIN_PRICE 
                               END 
                       END AS item_price,
                       CASE 
                           WHEN b.contract_no = '2' THEN 
                               CASE 
                                   WHEN d.PACKAGE_MODEL_TAX = 0 THEN d.MODEL_TAX 
                                   ELSE d.PACKAGE_MODEL_TAX 
                               END 
                           ELSE 
                               CASE 
                                   WHEN d.PACKAGE_CHAIN_TAX = 0 THEN d.CHAIN_TAX 
                                   ELSE d.PACKAGE_CHAIN_TAX 
                               END 
                       END AS item_tax,
                       CASE 
                            WHEN substr(tax_type,1,1) = '1' THEN 'tax' 
                            ELSE 'no tax' 
                       END AS tax
                FROM t_po_order_master AS a
                INNER JOIN cm_chain AS b ON a.chain_no = b.chain_no  
                INNER JOIN cm_chain AS c ON b.rechain_no = c.chain_no 
                INNER JOIN v_item_master AS d ON a.item_no = d.item_no 
                WHERE a.date = ?
            ) subquery;
        """

        # 데이터 추출 (using the original input date for extraction)
        df_step3 = extract_data(conn, query_step3, params=(formatted_input_date_str,))
        logging.info(f"3단계 데이터 추출 완료. 총 {len(df_step3)}개의 레코드.")

        # Override the 'date' column with the new date (input_date + 1) for insertion
        if not df_step3.empty:
            df_step3['date'] = formatted_new_date_str

        # Informix 연결 종료
        try:
            conn.close()
            logging.info("Informix 데이터베이스 연결 종료.")
        except Exception as e:
            logging.error(f"Informix 연결 종료 오류: {e}")

        # 데이터 엑셀로 저장
        try:
            step3_excel_path = os.path.join(OUTPUT_FOLDER, f'Step3_Data_{formatted_input_date_str}_{timestamp}.xlsx')
            save_to_excel(df_step3, step3_excel_path)
        except Exception as e:
            logging.error(f"엑셀 저장 과정에서 오류 발생: {e}")
            logging.error(traceback.format_exc())
            sys.exit(1)

        # MySQL 데이터베이스 연결
        mysql_conn = get_db_connection()
        if not mysql_conn:
            logging.error("MySQL 연결 실패로 ETL 프로세스를 중단합니다.")
            sys.exit(1)

        # MySQL에 데이터 삽입
        try:
            if not df_step3.empty:
                # ARTransactionsLedger 삽입 (수정 금지)
                df_ARTransactionsLedger = df_step3.groupby(['date', 'chain_no', 'full_name']).agg({'total': 'sum'}).reset_index()
                df_ARTransactionsLedger.rename(columns={
                    'chain_no': 'client',
                    'full_name': 'outlet_name',
                    'total': 'debit'
                }, inplace=True)
                df_ARTransactionsLedger['food_material_sales'] = df_ARTransactionsLedger['debit']
                df_ARTransactionsLedger['representative_code'] = ' '  # 공백 삽입
                df_ARTransactionsLedger['outlet_name'] = df_ARTransactionsLedger['outlet_name'].apply(convert_to_utf8)
                # Trim client and outlet_name
                df_ARTransactionsLedger['client'] = df_ARTransactionsLedger['client'].astype(str).str.strip()
                df_ARTransactionsLedger['outlet_name'] = df_ARTransactionsLedger['outlet_name'].astype(str).str.strip()
                df_ARTransactionsLedger = df_ARTransactionsLedger[['date', 'representative_code', 'client', 'outlet_name', 'debit', 'food_material_sales']]
                insert_mysql_ARTransactionsLedger(mysql_conn, df_ARTransactionsLedger)

                # AROrderDetails 삽입
                df_AROrderDetails = df_step3.groupby(['chain_no', 'full_name', 'date']).agg({'total': 'sum'}).reset_index()
                df_AROrderDetails.rename(columns={
                    'chain_no': 'client_code',
                    'full_name': 'client_name',
                    'total': 'order_amount'
                }, inplace=True)
                df_AROrderDetails['client_name'] = df_AROrderDetails['client_name'].apply(convert_to_utf8)
                df_AROrderDetails['representative_code'] = ' '  # 공백 삽입
                df_AROrderDetails['collector_key'] = ' '  # 공백 삽입
                df_AROrderDetails['manager'] = ' '  # 공백 삽입
                df_AROrderDetails.rename(columns={'date': 'order_date'}, inplace=True)
                df_AROrderDetails = df_AROrderDetails[['representative_code', 'client_code', 'client_name', 'collector_key', 'manager', 'order_date', 'order_amount']]
                insert_mysql_AROrderDetails(mysql_conn, df_AROrderDetails)

                # AROrderDetailsItem 삽입
                df_AROrderDetailsItem = df_step3.copy()
                df_AROrderDetailsItem.rename(columns={
                    'date': 'order_date',
                    'rep_full_name': 'rep_name',
                    'chain_no': 'client_code',
                    'full_name': 'client_name',
                    'item_no': 'item_code',
                    'item_full_name': 'item_name',
                    'item_price': 'unit_price',  # 필드 이름 변경
                    'item_tax': 'vat'            # 필드 이름 변경
                }, inplace=True)
                # order_amount 계산: total - vat
                df_AROrderDetailsItem['order_amount'] = df_AROrderDetailsItem['total'] - df_AROrderDetailsItem['vat']

                # client_name, rep_name, item_name, unit UTF-8 변환
                df_AROrderDetailsItem['client_name'] = df_AROrderDetailsItem['client_name'].apply(convert_to_utf8)
                df_AROrderDetailsItem['rep_name'] = df_AROrderDetailsItem['rep_name'].apply(convert_to_utf8)
                df_AROrderDetailsItem['item_name'] = df_AROrderDetailsItem['item_name'].apply(convert_to_utf8)
                df_AROrderDetailsItem['unit'] = df_AROrderDetailsItem['unit'].apply(convert_to_utf8)

                # cond 및 rep_code에 공백 삽입, cal_qty에 0.00 삽입
                df_AROrderDetailsItem['cond'] = ' '
                df_AROrderDetailsItem['rep_code'] = ' '  # 공백 삽입
                df_AROrderDetailsItem['cal_qty'] = 0.00

                # Instead of converting the order_date from the extracted data,
                # we force the order_date to be the same as used in AROrderDetails: formatted_new_date_str.
                df_AROrderDetailsItem['order_date'] = formatted_new_date_str

                df_AROrderDetailsItem['tax'] = df_AROrderDetailsItem['tax'].apply(
                    lambda x: '과세' if x.strip().lower() == 'tax' else ('면세' if x.strip().lower() == 'no tax' else x)
                )

                # 필요한 컬럼만 선택
                df_AROrderDetailsItem = df_AROrderDetailsItem[[ 
                    'order_date',
                    'rep_code',
                    'rep_name',
                    'client_code',
                    'client_name',
                    'item_code',
                    'item_name',
                    'cond',
                    'unit',
                    'qty',
                    'cal_qty',
                    'unit_price',
                    'order_amount',
                    'vat',
                    'total',
                    'tax'
                ]]
                df_AROrderDetailsItem.rename(columns={'total': 'total_amount'}, inplace=True)
                # Since order_date is already forced to formatted_new_date_str,
                # converting to string is unnecessary but we ensure it is a string.
                df_AROrderDetailsItem['order_date'] = df_AROrderDetailsItem['order_date'].astype(str)
                logging.info(f"AROrderDetailsItem 데이터프레임 컬럼명: {df_AROrderDetailsItem.columns.tolist()}")
                insert_mysql_AROrderDetailsItem(mysql_conn, df_AROrderDetailsItem)
            else:
                logging.warning("추출된 데이터가 비어있어 삽입을 건너뜁니다.")
        except Exception as e:
            logging.error(f"MySQL 데이터 삽입 과정에서 오류 발생: {e}")
            logging.error(traceback.format_exc())
            sys.exit(1)
        finally:
            try:
                mysql_conn.close()
                logging.info("MySQL 데이터베이스 연결 종료.")
            except Exception as e:
                logging.error(f"MySQL 연결 종료 오류: {e}")

        logging.info("ETL 프로세스가 성공적으로 완료되었습니다.")

    # 예외 처리 블록 (삭제 금지)
    except jaydebeapi.DatabaseError as db_err:
        logging.error(f"Database 에러 발생: {db_err}")
        logging.error(traceback.format_exc())
        raise db_err
    except Exception as e:
        logging.error(f"ETL 프로세스 실패: {e}")
        logging.error(traceback.format_exc())
        raise e  # 예외를 상위로 전달하여 추가적인 처리가 가능하도록 함

if __name__ == "__main__":
    etl_process()
