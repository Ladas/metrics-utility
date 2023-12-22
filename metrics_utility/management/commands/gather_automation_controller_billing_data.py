# import datetime

# from metrics_utility.base_command import BaseCommand
# from metrics_utility.automation_controller_billing.automation_controller_billing_collector import AutomationControllerBillingCollector

# class Command(BaseCommand):
#     help = 'This command is for gathering and shipping billing data to console.redhat.com'

#     def add_arguments(self, parser):
#         parser.add_argument('--since', type=datetime.datetime.fromisoformat, help='Start Date in ISO format YYYY-MM-DD')

#     def handle(self, *args, **options):
#         since = options.get('since')

#         if since is not None:
#             if since.tzinfo is None:
#                 since = since.replace(tzinfo=datetime.timezone.utc)


#         AutomationControllerBillingCollector(since=since).gather()

#         return None



import logging

from metrics_utility.automation_controller_billing.collector import Collector
from dateutil import parser
from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    """
    Gather AWX analytics data
    """

    help = 'Gather Automation Controller billing data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', dest='dry-run', action='store_true', help='Gather analytics without shipping. Works even if analytics are disabled in settings.'
        )
        parser.add_argument('--ship', dest='ship', action='store_true', help='Enable to ship metrics to the Red Hat Cloud')
        parser.add_argument('--since', dest='since', action='store', help='Start date for collection')
        parser.add_argument('--until', dest='until', action='store', help='End date for collection')

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def handle(self, *args, **options):
        self.init_logging()
        opt_ship = options.get('ship')
        opt_dry_run = options.get('dry-run')
        opt_since = options.get('since') or None
        opt_until = options.get('until') or None

        since = parser.parse(opt_since) if opt_since else None
        if since and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        until = parser.parse(opt_until) if opt_until else None
        if until and until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)

        if opt_ship and opt_dry_run:
            self.logger.error('Both --ship and --dry-run cannot be processed at the same time.')
            return
        collector = Collector(collection_type=Collector.MANUAL_COLLECTION if opt_ship else Collector.DRY_RUN)

        tgzfiles = collector.gather(since=since, until=until)
        if tgzfiles:
            for tgz in tgzfiles:
                self.logger.info(tgz)
        else:
            self.logger.error('No analytics collected')