from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import LocationViewSet, ServiceUpdateViewSet

router = DefaultRouter()
router.register(r"service-updates", ServiceUpdateViewSet)
router.register(r"locations", LocationViewSet)

urlpatterns = [path("", include(router.urls))]
