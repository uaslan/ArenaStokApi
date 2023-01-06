from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework import serializers, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.core.serializers import serialize
from .models import StockM
from stock.serializers import StockSerializer
from django.http import JsonResponse
from django.core import serializers
from django.db.models import Q

import datetime
import os
import psycopg2
import json
import redis


class Stock(APIView):
    # Allow any user (authenticated or not) to hit this endpoint.
    permission_classes = (AllowAny,)
    serializer_class = StockSerializer

    def get(self, request):
        hided = request.query_params.get('hided', 0)

        query = """
            with t as(
                select * from ag_product_parent
            )
            select t.parent_sku,st.total_qty
            from t
            inner join ag_products sp on sp.parent_id=t.id
            inner join ag_stock st on st.product_id=t.id
            group by t.parent_sku,st.total_qty
            order by t.parent_sku;
        """
        cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
        cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        query_response = cursor.fetchall()
        cursor.close()
        cnxn.close()

        stocks = []
        for item in query_response:
            row = {
                'parent_sku': item['parent_sku'],
                'name': str(item['parent_sku']).replace('_main','').replace('_',' '),
                'total_qty': item['total_qty']
            }
            stocks.append(row)
        
        return JsonResponse({'products': stocks, 'productsCount': int(len(query_response))}, safe=False)




def db_process(product_list,type):
    cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
    cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    bulkSql="""
        TRUNCATE TABLE tmp_stock_initial;
        INSERT INTO tmp_stock_initial (product_id,total_qty,avaliable_qty,reserved_qty) VALUES """
    bulkLogsSql="INSERT INTO ag_stock_logs (created_at,product_id,quantity,description,process_type) VALUES "
    initial_desc=f"{datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')} - Stok Girisi - {type}"
    for item in product_list:
        bulkSql+=f"({item['product_id']},{item['total_qty']},{item['avaliable_qty']},{item['reserved_qty']}),"
        bulkLogsSql+=f"(now(),{item['product_id']},{item['total_qty']},'{initial_desc}','initial-api'),"
    bulkSql=bulkSql[:-1]
    bulkLogsSql=bulkLogsSql[:-1]
    
    cursor.execute(bulkSql)
    cursor.execute(bulkLogsSql)
    cnxn.commit()

    cursor.execute("select * from tmp_stock_initial")
    update_rows=cursor.fetchall()
    for row in update_rows:
        if type=='ekle':
            update_sql=f"""
                UPDATE ag_stock AS ss
                SET
                updated_at=now(),
                total_qty =ss.total_qty + {row['total_qty']},
                avaliable_qty =ss.avaliable_qty + {row['avaliable_qty']},
                reserved_qty =ss.reserved_qty + {row['reserved_qty']}
                WHERE ss.product_id = {row['product_id']};
            """
            cursor.execute(update_sql)
            cnxn.commit()
        else:
            update_sql=f"""
                UPDATE ag_stock AS ss
                SET
                updated_at=now(),
                total_qty = {row['total_qty']},
                avaliable_qty = {row['avaliable_qty']},
                reserved_qty = {row['reserved_qty']}
                WHERE ss.product_id = {row['product_id']};
            """
            cursor.execute(update_sql)
            cnxn.commit()

    cursor.close()
    cnxn.close()
    

@api_view(['POST'])
def replace_stocks(request):
    if request.method == 'POST':
        r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True,password=os.getenv('redis_password'))
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8",password=os.getenv('redis_password'))
        payload = request.data

        product_list=[]
        error=0
        for item in payload:
            try:
                parent_sku=None
                parent_sku=f"{item['sku']}"
                if parent_sku!=None:
                    product_maps_id=r1.get(parent_sku)
                    stock_qty=item['quantity']
                    if stock_qty!=None and stock_qty!='':
                        product_list.append({'product_id':product_maps_id,'total_qty':stock_qty,'avaliable_qty':stock_qty,'reserved_qty':stock_qty})
                else:
                    error+=1
            except Exception as for_error:
                pass

        if len(product_list)>0:
            process_type=''
            try:
                db_process(product_list,process_type)
                response_data = {
                    "success": True
                }
                r1.set('update_check','true')
            except:
                response_data = {
                    "success": False
                }
        return Response(response_data, status=status.HTTP_200_OK)

@api_view(['POST'])
def add_stocks(request):
    if request.method == 'POST':
        r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True,password=os.getenv('redis_password'))
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8",password=os.getenv('redis_password'))
        payload = request.data

        product_list=[]
        error=0
        for item in payload:
            try:
                parent_sku=None
                parent_sku=f"{item['sku']}"
                if parent_sku!=None:
                    product_maps_id=r1.get(parent_sku)
                    stock_qty=item['quantity']
                    if stock_qty!=None and stock_qty!='':
                        product_list.append({'product_id':product_maps_id,'total_qty':stock_qty,'avaliable_qty':stock_qty,'reserved_qty':stock_qty})
                else:
                    error+=1
            except Exception as for_error:
                pass

        if len(product_list)>0:
            process_type='ekle'
            try:
                db_process(product_list,process_type)
                response_data = {
                    "success": True
                }
                r1.set('update_check','true')
            except:
                response_data = {
                    "success": False
                }
        return Response(response_data, status=status.HTTP_200_OK)

@api_view(['GET'])
def stock_logs(request):
    parent_sku = request.query_params.get('sku',None)
    if parent_sku!=None:
        logs = []
        r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True,password=os.getenv('redis_password'))
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8",password=os.getenv('redis_password'))
        product_maps_id=r1.get(parent_sku)
        if product_maps_id!=None:
            query = f"""
                select created_at,quantity,description,process_type,product_id from ag_stock_logs 
                where product_id={product_maps_id}
                order by created_at desc;
            """
            cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
            cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query)
            query_response = cursor.fetchall()

            cursor.execute(f"select * from ag_stock where product_id={product_maps_id}")
            sku_stock = cursor.fetchone()
            
            cursor.close()
            cnxn.close()
        
            total_qty=0
            for item in query_response:
                row = {
                    'created_at':str(item['created_at']).split('.')[0],
                    'parent_sku': parent_sku,
                    'qty': item['quantity'],
                    'description':item['description'],
                    'type':item['process_type']
                }
                logs.append(row)
                total_qty+=int(item['quantity'])
        return JsonResponse({'logs': logs,'sum':total_qty,'total_stock':sku_stock['total_qty']}, safe=False)
    else:
        return Response({"error":"Sku Bulunamadi"})

@api_view(['GET'])
def all_stock_logs(request):
    logs = []
    query = f"""
        select app.parent_sku,sl.created_at,quantity,description,process_type,product_id 
        from ag_stock_logs sl
        left join ag_product_parent app on app.id=sl.product_id
        where process_type='initial-api'
        order by created_at desc;
    """
    cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
    cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(query)
    query_response = cursor.fetchall()    
    cursor.close()
    cnxn.close()

    for item in query_response:
        row = {
            'parent_sku': item['parent_sku'],
            'created_at':str(item['created_at']).split('.')[0],
            'qty': item['quantity'],
            'description':item['description'],
            'type':item['process_type']
        }
        logs.append(row)
    return JsonResponse({'logs': logs}, safe=False)