from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework import serializers, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.core.serializers import serialize
from .models import OrderM
from stock.serializers import StockSerializer
from django.http import JsonResponse
from django.core import serializers
from django.db.models import Q

import datetime
import os
import psycopg2
import json
import redis

r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
r2Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=2, decode_responses=True)

class Order(APIView):
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


def process_orders(process_list):
    r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8")
    cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
    cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        bulkSql="""
            INSERT INTO ag_orders (created_at,updated_at,order_number,order_status,order_date,order_marketplace_id) VALUES """
        for order,items in process_list.items():
            bulkSql+=f"(now(),now(),'{items['order_number']}','{items['order_status']}','{str(items['order_date']).replace('.','-')}',{items['order_marketplace_id']}),"
        bulkSql=bulkSql[:-1]+" returning order_number;"
        cursor.execute(bulkSql)
        order_numbers=cursor.fetchall()
        cnxn.commit()
        for order_number in order_numbers:
            r2.set(f"order_number_{order_number['order_number']}",1)
    except Exception as error:
        print(error)
    cursor.close()
    cnxn.close()

def db_product_order(product_list):
    try:
        resp=None
        r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8")
        cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
        cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        bulkSql="""
            INSERT INTO ag_order_products (created_at,updated_at,order_number,order_product_qty,product_id,price,amount,discount) VALUES """
        for product in product_list:
            bulkSql+=f"(now(),now(),'{product['order_number']}',{product['order_product_qty']},{product['product_id']},{product['price']},{product['amount']},{product['discount']}),"
        bulkSql=bulkSql[:-1]
        if bulkSql.find("VALUES (")>-1:
            cursor.execute(bulkSql)
            cnxn.commit()

        bulkSql="""
            TRUNCATE TABLE tmp_stock_initial;
            INSERT INTO tmp_stock_initial (product_id,total_qty,avaliable_qty,reserved_qty) VALUES """
        bulkLogsSql="INSERT INTO ag_stock_logs (created_at,product_id,quantity,description,process_type) VALUES "
        for product in product_list:
            stock_check=r2.get(f"stock_xls_{product['order_number']}_{product['line_id']}")
            if stock_check==None:
                bulkSql+=f"({product['product_id']},{product['order_product_qty']},{product['order_product_qty']},{product['order_product_qty']}),"
                bulkLogsSql+=f"(now(),{product['product_id']},-{product['order_product_qty']},'{product['order_number']}','order-xls'),"
                r2.set(f"stock_xls_{product['order_number']}_{product['line_id']}",'Order')
        bulkSql=bulkSql[:-1]
        bulkLogsSql=bulkLogsSql[:-1]

        if bulkSql.find("VALUES (")>-1:
            cursor.execute(bulkSql)
            cnxn.commit()
            cursor.execute(bulkLogsSql)
            cnxn.commit()

            bulkUpdateStockSql=""";
                UPDATE ag_stock AS ss
                SET
                total_qty =ss.total_qty - si.total_qty,
                avaliable_qty =ss.avaliable_qty - si.avaliable_qty,
                reserved_qty =ss.reserved_qty - si.reserved_qty
                FROM tmp_stock_initial si
                WHERE ss.product_id = si.product_id;
            """
            cursor.execute(bulkUpdateStockSql)
            cnxn.commit()

        cursor.close()
        cnxn.close()
    except Exception as error:
        print(error)
    
@api_view(['POST'])
def add_orders(request):
    if request.method == 'POST':
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8")
        r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8")
        payload = request.data
        order_list={}
        order_product_list=[]
        for item in payload:
            try:
                item['order_number']=str(item['order_number'])
                order_number=r2.get(f"order_number_{item['order_number']}")
                if order_number==None:
                    marketplace_id=r2.hget(f"marketplace_{item['marketplace']}",'id')
                    try:
                        if order_list[item['order_number']]==None:
                            order_list[item['order_number']]={}
                    except:
                        order_list[item['order_number']]={}

                    order_list[item['order_number']]['order_number']=item['order_number']
                    order_list[item['order_number']]['order_status']=item['order_status']
                    order_list[item['order_number']]['order_date']=item['order_date']
                    order_list[item['order_number']]['order_marketplace_id']=marketplace_id
                    if item['parent_sku']=='Black_S':
                        continue
                    parent_sku=None
                    parent_sku=f"{item['marketplace']}_{item['parent_sku']}"
                    parent_sku=r1.get(parent_sku)
                    if parent_sku!=None:
                        product_maps_id=r1.get(parent_sku)
                        item={
                            'order_number':item['order_number'],
                            'order_product_qty':item['order_product_qty'],
                            'product_id':product_maps_id,
                            'price':float(item['price']),
                            'amount':float(item['amount']),
                            'discount':float(item['discount']),
                            'order_status':item['order_status'],
                            'line_id':item['order_number']+"_"+product_maps_id
                        }
                        order_product_list.append(item)
                    else:
                        print(f"{item['marketplace']}_{item['parent_sku']}")
                else:
                    print(f"{item['order_number']}_{item['parent_sku']} - Mevcut")
            except Exception as for_error:
                pass
        
        response_data = {
            "success": False
        }
        if len(order_list)>0:
            try:
                process_orders(order_list)
                stockData=db_product_order(order_product_list)
                response_data = {
                    "success": True,
                }
                r1.set('update_check','true')
            except:
                response_data = {
                    "success": False
                }
        return Response(response_data, status=status.HTTP_200_OK)

