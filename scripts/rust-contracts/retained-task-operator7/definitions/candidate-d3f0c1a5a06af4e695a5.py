





class Command(BaseCommand):
    help = "Reset network access settings"

    def handle(self, *args, **options):
        setting = CoreSettings.objects.get(key=NETWORK_ACCESS_KEY)
        setting.value = {}
        setting.save()
