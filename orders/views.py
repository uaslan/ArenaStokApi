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

from datetime import datetime
import os
import psycopg2
import json
import redis
import math

r1Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1, decode_responses=True,password=os.getenv('redis_password'))
r2Pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=2, decode_responses=True,password=os.getenv('redis_password'))

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
    r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8",password=os.getenv('redis_password'))
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
        r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8",password=os.getenv('redis_password'))
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
                bulkLogsSql+=f"(now(),{product['product_id']},-{product['order_product_qty']},'{product['order_number']}','{product['marketplace']}'),"
                r2.set(f"stock_xls_{product['order_number']}_{product['line_id']}",'Order')
        bulkSql=bulkSql[:-1]
        bulkLogsSql=bulkLogsSql[:-1]

        if bulkSql.find("VALUES (")>-1:
            cursor.execute(bulkSql)
            cnxn.commit()
            cursor.execute(bulkLogsSql)
            cnxn.commit()

            cursor.execute("select * from tmp_stock_initial")
            update_rows=cursor.fetchall()
            for row in update_rows:
                update_sql=f"""
                    UPDATE ag_stock AS ss
                    SET
                    updated_at=now(),
                    total_qty =ss.total_qty - {row['total_qty']},
                    avaliable_qty =ss.avaliable_qty - {row['avaliable_qty']},
                    reserved_qty =ss.reserved_qty - {row['reserved_qty']}
                    WHERE ss.product_id = {row['product_id']};
                """
                cursor.execute(update_sql)
                cnxn.commit()

        cursor.close()
        cnxn.close()
    except Exception as error:
        print(error)
    
@api_view(['POST'])
def add_orders(request):
    if request.method == 'POST':
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8",password=os.getenv('redis_password'))
        r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8",password=os.getenv('redis_password'))
        payload = request.data
        order_list={}
        order_product_list=[]
        for item in payload:
            try:
                item['order_number']=str(item['order_number'])
                order_number=r2.get(f"order_number_{item['order_number']}")
                # pass_list=['8USXAUZY','8HMPLVGM','6UDK9TZV','5Q3UCL3I','3WUX28BY','1FXSW46J']
                # if item['order_number'] in pass_list:
                #     print('pass')
                #     order_number=None
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
                            'line_id':item['order_number']+"_"+product_maps_id,
                            'marketplace':item['marketplace']
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

@api_view(['GET'])
def report(request):
    if request.method == 'GET':
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8",password=os.getenv('redis_password'))
        r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8",password=os.getenv('redis_password'))

        first_date = request.query_params.get('start_date', '')
        end_date = request.query_params.get('end_date', '')
        cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
        cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if first_date=='' and end_date=='':
            query = """
                with t as(
                    select order_number,order_status,order_date,order_marketplace_id from ag_orders where order_date>now()-INTERVAL '30 day'
                )
                select t.order_number,t.order_status,t.order_date,mp.symbol,pp.parent_sku,op.order_product_qty,(now()-INTERVAL '15 day') start_date,pp.collection_name,op.shipment_company,op.package_number
                ,od.shippingaddress_city,od.shippingAddress_town,od.cargoCompany
                from t
                inner join ag_order_products op on op.order_number=t.order_number
                inner join ag_product_parent pp on pp.id=op.product_id
                inner join ag_marketplaces mp on mp.id=t.order_marketplace_id
                left join ag_orders_detail od ON od.ordernumber=t.order_number
                order by t.order_date desc
            """
            cursor.execute(query)
        else:
            if end_date=='':
                end_date='now()'
            # if first_date=='':
            #     first_date="now()-INTERVAL '30 day'"
            query = """
                with t as(
                    select order_number,order_status,order_date,order_marketplace_id from ag_orders where order_date>=%(first_date)s::TIMESTAMP and order_date<=%(end_date)s::TIMESTAMP + INTERVAL '1 days'
                )
                select t.order_number,t.order_status,t.order_date,mp.symbol,pp.parent_sku,op.order_product_qty,(now()-INTERVAL '15 day') start_date,pp.collection_name,op.shipment_company,op.package_number
                ,od.shippingaddress_city,od.shippingAddress_town,od.cargoCompany
                from t
                inner join ag_order_products op on op.order_number=t.order_number
                inner join ag_product_parent pp on pp.id=op.product_id
                inner join ag_marketplaces mp on mp.id=t.order_marketplace_id
                left join ag_orders_detail od ON od.ordernumber=t.order_number
                order by t.order_date desc
            """
            cursor.execute(query,{'first_date':first_date,'end_date':end_date})
        query_response = cursor.fetchall()
        cursor.close()
        cnxn.close()
        report_list={}
        report_list['data']=[]
        report_list['visible']=[]
        report_list['unvisible']=[]
        for item in query_response:
            try:
                product=str(item['parent_sku']).replace('_main','').split('_')
                if len(product)>3:
                    collection=product[0]
                    model=product[1]
                    color=product[2]
                    size=product[3]
                elif len(product)>2:
                    collection=product[0]
                    model=product[0]
                    color=product[1]
                    size=product[2]
                elif str(item['parent_sku']).find('ShopperBag')>-1:
                    collection=product[0]
                    model=product[1]
                    color='No Color'
                    size='No Size'
                else:
                    collection='Shades'
                    model='Shades'
                    corap_list=["Uzun","Orta","Patik"]
                    if product[0] in corap_list:
                        collection='Corap'
                        model=product[0]
                        color=product[1]
                        size=product[0]
                    else:
                        color=product[0]
                        size=product[1]

                if item['collection_name']!=None :
                    collection=item['collection_name']
                    
                start_date_str=str(item['start_date']).replace('T',' ').split(' ')[0]
                start_date=datetime.strptime(start_date_str,"%Y-%m-%d")

                date_time_str=str(item['order_date']).replace('T',' ').split(' ')[0]
                order_date=datetime.strptime(date_time_str,"%Y-%m-%d")

                if order_date>=start_date:
                    report_list['visible'].append(str(order_date.strftime("%Y-%m-%d")))
                else:
                    report_list['unvisible'].append(str(order_date.strftime("%Y-%m-%d")))
                row = {
                    'OrderNO': item['order_number'],
                    'OrderStatus': item['order_status'],
                    'SKU': str(item['parent_sku']).replace('_main',''),
                    'Collection':collection,
                    'Model':model,
                    'Color':color,
                    'Size':size,
                    'OrderDate':str(item['order_date']).replace('T',' ').split(' ')[0],
                    'OMonth':str(order_date.strftime("%b")),
                    'OWeek':str(order_date.strftime("%W")),
                    'ODay':str(order_date.strftime("%d")),
                    'Marketplace':item['symbol'],
                    'Quantity':item['order_product_qty'],
                    'PackageNumber':item['package_number'],
                    'ShipCompany':item['shipment_company'],
                    'ShipCity':item['shippingaddress_city'],
                    'ShipTown':item['shippingaddress_town'],
                    'CargoCompany':item['cargocompany']
                    # 'RMonth':None,
                    # 'RWeek':None,
                    # 'RDay':None,
                    # 'RReason':'',
                    # 'RStatus':'',
                    # 'RDate':'',
                }
                report_list['data'].append(row)
            except Exception as error:
                print(error)

        report_list['visible']=set(report_list['visible'])
        report_list['unvisible']=set(report_list['unvisible'])
        return Response(report_list,status=status.HTTP_200_OK)

def ceiling(number, significance = 1):
    try:
        return math.ceil(number/significance) * significance
    except:
        return None

@api_view(['GET'])
def velocity(request):
    if request.method == 'GET':
        r1 = redis.Redis(connection_pool=r1Pool,charset="utf-8",password=os.getenv('redis_password'))
        r2 = redis.Redis(connection_pool=r2Pool,charset="utf-8",password=os.getenv('redis_password'))
        query = """
            with f as(
                    with t as(
                        select * from ag_product_parent --where parent_sku LIKE 'Shades%'
                    )
                    select t.id p_id,t.parent_sku,s.total_qty,t.collection_name
                    from t
                    inner join ag_stock s on s.product_id=t.id
            )
            select f.parent_sku,f.total_qty,sum(op.order_product_qty) total_sales,o.order_number,f.collection_name
            from f
            left join ag_order_products op ON op.product_id=f.p_id
            left join ag_orders o on o.order_number = op.order_number and o.order_date>now() - INTERVAL '30 days' and o.order_status!='Cancelled'
            GROUP BY f.parent_sku,f.total_qty,o.order_number,f.collection_name
            order by f.parent_sku asc
        """
        cnxn = psycopg2.connect(user=os.getenv('DATABASE_USER'),password=os.getenv('DATABASE_PASSWORD'),host=os.getenv('DATABASE_HOST'),port=os.getenv('DATABASE_PORT'),database=os.getenv('DATABASE_NAME'))
        cursor =cnxn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        query_response = cursor.fetchall()
        cursor.close()
        cnxn.close()

        temp_list={}
        for item in query_response:
            try:
                keys=item['parent_sku'].replace('_main','').replace('_New','').split('_')
                if len(keys)>3:
                    collection=keys[0]
                    model=keys[1]
                    color=keys[2]
                    size=keys[3]
                elif len(keys)>2:
                    collection=keys[0]
                    model=keys[0]
                    color=keys[1]
                    size=keys[2]
                elif str(item['parent_sku']).find('ShopperBag')>-1:
                    collection=keys[0]
                    model=keys[1]
                    color='No Color'
                    size='No Size'
                else:
                    collection='Shades'
                    model='Shades'
                    corap_list=["Uzun","Orta","Patik"]
                    if keys[0] in corap_list:
                        collection='Corap'
                        model=keys[0]
                        color=keys[1]
                        size=keys[0]
                    else:
                        color=keys[0]
                        size=keys[1]
                
                if item['collection_name']!=None:
                    model=f"{item['collection_name']}_{model}"

                if model not in temp_list:
                    temp_list[model]={}
                if color not in temp_list[model]:
                    temp_list[model][color]={}

                if size not in temp_list[model][color]:
                    temp_list[model][color][size]={}
                    temp_list[model][color][size]['7days']=0
                    temp_list[model][color][size]['speed']= 0
                    temp_list[model][color][size]['oos'] = 0
                    temp_list[model][color][size]['order'] = 0
                    temp_list[model][color][size]['production'] = 0
                    temp_list[model][color][size]['stock'] = 0
                
                temp_list[model][color][size]['stock'] = item['total_qty']
                try:
                    if item['total_sales']!=None and item['order_number']!=None:
                        doi=30
                        temp_list[model][color][size]['7days']+=item['total_sales']
                        temp_list[model][color][size]['speed']=round(float(temp_list[model][color][size]['7days']/30),2)
                        temp_list[model][color][size]['oos'] = int((item['total_qty'] / temp_list[model][color][size]['speed']))
                        temp_list[model][color][size]['order'] = ceiling(int((doi * temp_list[model][color][size]['speed']) - item['total_qty']),50)
                        temp_list[model][color][size]['production'] = temp_list[model][color][size]['order']
                except Exception as error:
                    print(error)
            except Exception as error:
                print(error)
        report_list={}
        newSizeSort = ["XXS","XS", "S", "SM", "M", "L", "LXL", "XL", "XXL", "3XL",'ML','XLXXL','XSS']
        for model,temp_item_model in temp_list.items():
            for color,temp_item_color in temp_item_model.items():
                for sort_size in newSizeSort:
                    if model not in report_list:
                        report_list[model]={}
                    if color not in report_list[model]:
                        report_list[model][color]={}
                        report_list[model][color]['Total']={}
                        report_list[model][color]['Total']['7days']=0
                        report_list[model][color]['Total']['speed']= 0
                        report_list[model][color]['Total']['oos'] = 0
                        report_list[model][color]['Total']['order'] = 0
                        report_list[model][color]['Total']['production'] = 0
                        report_list[model][color]['Total']['stock'] = 0
                    if sort_size in temp_list[model][color]:
                        report_list[model][color][sort_size]=temp_item_model[color][sort_size]
                        try:
                            report_list[model][color]['Total']['7days'] += temp_item_model[color][sort_size]['7days']
                            report_list[model][color]['Total']['speed'] += temp_item_model[color][sort_size]['speed']
                            report_list[model][color]['Total']['oos'] += temp_item_model[color][sort_size]['oos']
                            report_list[model][color]['Total']['order'] += temp_item_model[color][sort_size]['order']
                            report_list[model][color]['Total']['production'] += temp_item_model[color][sort_size]['production']
                            report_list[model][color]['Total']['stock'] += temp_item_model[color][sort_size]['stock']
                        except:
                            pass
        try:
            keys = sorted(report_list.keys())
            new_list={}
            for key in keys:
                new_list[key] = report_list[key]
            return Response(new_list,status=status.HTTP_200_OK)
        except Exception as error:
            pass


        return Response(report_list,status=status.HTTP_200_OK)
