from symtable import Symbol
from rest_framework import serializers


from .models import StockM


class StockSerializer(serializers.ModelSerializer):

    class Meta:
        model = StockM
        # List all of the fields that could possibly be included in a request
        # or response, including fields specified explicitly above.
        fields = "__all__"

    def create(self, validated_data):
        # Use the `create_user` method we wrote earlier to create a new user.
        return StockM.objects.create(**validated_data)

        
