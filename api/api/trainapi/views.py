from django.shortcuts import render
from rest_framework import viewsets

from .models import Location, ServiceUpdate
from .serializers import LocationSerializer, ServiceUpdateSerializer


class ServiceUpdateViewSet(viewsets.ModelViewSet):

    queryset = ServiceUpdate.objects.all()
    serializer_class = ServiceUpdateSerializer


class LocationViewSet(viewsets.ModelViewSet):

    queryset = Location.objects.all()
    serializer_class = LocationSerializer
