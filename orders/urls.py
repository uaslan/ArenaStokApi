from django.urls import include, path

from .views import Order, add_orders,report,velocity

app_name = 'orders'
urlpatterns = [
    path('orders/', Order.as_view()),
    path('order_upload/', add_orders),
    path('report/', report),
    path('velocity/', velocity)
]
