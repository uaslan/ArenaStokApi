from django.db import models

from core.models import TimestampedModel


class OrderM(TimestampedModel):
    order_number = models.CharField(db_index=True, max_length=255)
    order_status = models.CharField(db_index=True, max_length=255)
    order_date = models.DateTimeField()
    order_marketplace_id = models.IntegerField()

    class Meta:
        managed = False
        db_table = u'ag_orders'
        
    def __str__(self):
        return self.name
