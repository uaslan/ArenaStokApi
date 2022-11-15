from django.urls import include, path

from .views import Stock, add_stocks, replace_stocks ,stock_logs

app_name = 'stock'
urlpatterns = [
    path('stocks/', Stock.as_view()),
    path('replace_stocks/', replace_stocks),
    path('add_stocks/', add_stocks),
    path('stock_logs/', stock_logs)
]
