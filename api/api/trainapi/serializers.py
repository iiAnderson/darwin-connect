from rest_framework import serializers

from .models import Location, ServiceUpdate


class LocationSerializer(serializers.ModelSerializer):

    service_update_id = serializers.PrimaryKeyRelatedField(queryset=ServiceUpdate.objects.all())

    class Meta:
        model = Location
        fields = "__all__"


class ServiceUpdateSerializer(serializers.ModelSerializer):

    locations = LocationSerializer(many=True, read_only=True)

    class Meta:
        model = ServiceUpdate
        fields = "__all__"
