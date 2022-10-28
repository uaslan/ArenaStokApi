from django.urls import include, path

from .views import Stock, replace_stocks

app_name = 'stock'
urlpatterns = [
    path('stocks/', Stock.as_view()),
    path('replace_stocks/', replace_stocks)
]
