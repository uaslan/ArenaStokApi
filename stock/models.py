from django.db import models

from core.models import TimestampedModel


class StockM(TimestampedModel):
    product_id = models.IntegerField()
    total_qty = models.IntegerField()
    available_qty = models.IntegerField()
    reserved_qty = models.IntegerField()

    class Meta:
        managed = False
        db_table = u'ag_stock'
        
    def __str__(self):
        return self.name
