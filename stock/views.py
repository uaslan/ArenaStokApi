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
                select id,updated_at,product_id,total_qty from ag_stock --where product_id=630
            )
            select spp.id parent_id,t.updated_at,spp.parent_sku,sm.symbol,t.total_qty,sp.sku,sp.second_sku,sp.barcode
            from t
            inner join ag_product_parent spp on spp.id=t.product_id
            inner join ag_products sp on sp.parent_id=spp.id
            inner join ag_marketplaces sm on sm.id=sp.marketplace_id
            order by spp.parent_sku;
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
                'barcode': item['barcode'],
                'sku': item['sku'],
                'second_sku': item['second_sku'],
                'total_qty': item['total_qty']
            }
            stocks.append(item)
        
        return JsonResponse({'products': stocks, 'productsCount': int(len(query_response))}, safe=False)




def db_process(product_list,type):
    cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
    cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    bulkSql="""
        TRUNCATE TABLE tmp_stock_initial;
        INSERT INTO tmp_stock_initial (product_id,total_qty,avaliable_qty,reserved_qty) VALUES """
    bulkLogsSql="INSERT INTO ag_stock_logs (created_at,product_id,quantity,description,process_type) VALUES "
    initial_desc=f"{datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')} - Stok Girisi"
    for item in product_list:
        bulkSql+=f"({item['product_id']},{item['total_qty']},{item['avaliable_qty']},{item['reserved_qty']}),"
        bulkLogsSql+=f"(now(),{item['product_id']},{item['total_qty']},'{initial_desc}','initial-api'),"
    bulkSql=bulkSql[:-1]
    bulkLogsSql=bulkLogsSql[:-1]
    cursor.execute(bulkSql)
    cursor.execute(bulkLogsSql)
    cnxn.commit()
    if type=='ekle':
        bulkSql=""";
            UPDATE ag_stock AS ss
            SET
            total_qty =ss.total_qty+si.total_qty,
            avaliable_qty =ss.avaliable_qty+si.avaliable_qty,
            reserved_qty =ss.reserved_qty+si.reserved_qty
            FROM tmp_stock_initial si
            WHERE ss.product_id = si.product_id;
        """
    else:
        bulkSql=""";
            UPDATE ag_stock AS ss
            SET
            total_qty = si.total_qty,
            avaliable_qty = si.avaliable_qty,
            reserved_qty = si.reserved_qty
            FROM tmp_stock_initial si
            WHERE ss.product_id = si.product_id;
        """
    cursor.execute(bulkSql)
    cnxn.commit()
    cursor.close()
    cnxn.close()
    

@api_view(['POST'])
def replace_stocks(request):
    if request.method == 'POST':
        r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8")
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
            except:
                response_data = {
                    "success": False
                }
        return Response(response_data, status=status.HTTP_200_OK)

@api_view(['POST'])
def add_stocks(request):
    if request.method == 'POST':
        r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True)
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8")
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
            except:
                response_data = {
                    "success": False
                }
        return Response(response_data, status=status.HTTP_200_OK)