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

from datetime import datetime
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


# @api_view(['GET'])
# def getCollectionObj(request):
#     if request.method == 'GET':
#         password = request.query_params.get("password")
#         symbol = request.query_params.get("symbol")
        
#         if password != "zCpdCWUf":
#             return Response(status=status.HTTP_400_BAD_REQUEST)

#         r1 = redis.Redis(host='127.0.0.1', port=6379, db=1,charset="utf-8", decode_responses=True)
#         redis_collection_obj = r1.hgetall(symbol)
#         if len(redis_collection_obj)>0:
#             response_data = {
#                 "collection_details": redis_collection_obj
#             }
#             return Response(response_data, status=status.HTTP_200_OK)
#         else:
#             return Response({'error':True,'message':'Collection Not Found'}, status=status.HTTP_400_BAD_REQUEST)

def pay_round(pay):
    try:
        if float(pay)>99.9:
            return round(float(pay),0)
        elif float(pay)>9.99:
            return round(float(pay),1)
        elif float(pay)>0.999:
            return round(float(pay),2)
        else:
            return round(float(pay),3)
    except:
        return round(float(pay),3)