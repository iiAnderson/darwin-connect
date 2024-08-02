import uuid

from django.db import models


class ServiceUpdate(models.Model):

    update_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ts = models.DateTimeField()
    rid = models.CharField(max_length=30)
    uid = models.CharField(max_length=10)
    passenger = models.BooleanField()

    class Meta:
        db_table = "service_updates"
        indexes = [models.Index(fields=["ts"]), models.Index(fields=["rid"]), models.Index(fields=["uid"])]


class Location(models.Model):

    update_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    service_update = models.ForeignKey(ServiceUpdate, on_delete=models.CASCADE, related_name="locations")
    time = models.TimeField()
    tpl = models.CharField(max_length=10)
    type = models.CharField(max_length=10)

    class Meta:
        db_table = "locations"
        indexes = [models.Index(fields=["time"]), models.Index(fields=["tpl"]), models.Index(fields=["type"])]
