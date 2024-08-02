from django.contrib import admin

from .models import Location, ServiceUpdate

admin.site.register(ServiceUpdate)
admin.site.register(Location)
