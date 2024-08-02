# Generated by Django 5.0.7 on 2024-08-02 18:58

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("trainapi", "0002_alter_location_service_update_and_more"),
    ]

    operations = [
        migrations.RenameIndex(
            model_name="location",
            new_name="locations_time_99626f_idx",
            old_name="trainapi_lo_time_c26bca_idx",
        ),
        migrations.RenameIndex(
            model_name="location",
            new_name="locations_tpl_51d20c_idx",
            old_name="trainapi_lo_tpl_7dc0ee_idx",
        ),
        migrations.RenameIndex(
            model_name="location",
            new_name="locations_type_f5c4b7_idx",
            old_name="trainapi_lo_type_39a42c_idx",
        ),
        migrations.RenameIndex(
            model_name="serviceupdate",
            new_name="service_upd_ts_f4fbac_idx",
            old_name="trainapi_se_ts_1d39f5_idx",
        ),
        migrations.RenameIndex(
            model_name="serviceupdate",
            new_name="service_upd_rid_6341f8_idx",
            old_name="trainapi_se_rid_8eb72d_idx",
        ),
        migrations.RenameIndex(
            model_name="serviceupdate",
            new_name="service_upd_uid_6eaf10_idx",
            old_name="trainapi_se_uid_c087ef_idx",
        ),
        migrations.AlterModelTable(
            name="location",
            table="locations",
        ),
        migrations.AlterModelTable(
            name="serviceupdate",
            table="service_updates",
        ),
    ]
