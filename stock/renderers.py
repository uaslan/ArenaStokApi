from core.renderers import AGJsonRenderer


class StockJSONRenderer(AGJsonRenderer):
    object_label = 'stock'
    pagination_object_label = 'stock'
    pagination_count_label = 'stockCount'
