from django.urls import include, path

from .views import *

app_name = 'stock'
urlpatterns = [
    path('stocks/', Stock.as_view()),
    path('replace_stocks/', replace_stocks),
    path('add_stocks/', add_stocks),
    path('stock_logs/', stock_logs),
    path('all_stock_logs/', all_stock_logs)
]
