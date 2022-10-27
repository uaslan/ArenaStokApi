from django.urls import include, path

from .views import Stock

app_name = 'stock'
urlpatterns = [
    path('stocks/', Stock.as_view()),
    # path('get_collection/', getCollectionObj),
]
