from core.renderers import AGJsonRenderer


class OrdersJSONRenderer(AGJsonRenderer):
    object_label = 'orders'
    pagination_object_label = 'orders'
    pagination_count_label = 'ordersCount'
