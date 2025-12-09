"""Management command to sync MLS data.

This command is designed to be run by Heroku Scheduler at midnight
for incremental data updates.
"""

import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from django.utils import timezone

from sales.tasks import (
    calculate_agent_stats,
    run_full_sync,
    sync_members,
    sync_properties,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Django management command for syncing MLS data.

    This command synchronizes member and property data from the WFRMLS API.
    By default, it performs an incremental sync (only new/modified records).
    Use --full flag for a complete data refresh.

    Examples:
        # Incremental sync (default)
        python manage.py sync_mls_data

        # Full sync
        python manage.py sync_mls_data --full

        # Sync specific year
        python manage.py sync_mls_data --year 2024

        # Only sync members
        python manage.py sync_mls_data --members-only

        # Only sync properties
        python manage.py sync_mls_data --properties-only

        # Only recalculate stats
        python manage.py sync_mls_data --stats-only
    """

    help = "Synchronize MLS data from WFRMLS API"

    def add_arguments(self, parser: CommandParser) -> None:
        """Add command-line arguments.

        Args:
            parser: The argument parser to add arguments to.
        """
        parser.add_argument(
            "--full",
            action="store_true",
            help="Perform a full sync instead of incremental",
        )
        parser.add_argument(
            "--year",
            type=int,
            default=None,
            help="Year to sync properties for (default: current year)",
        )
        parser.add_argument(
            "--members-only",
            action="store_true",
            help="Only sync members",
        )
        parser.add_argument(
            "--properties-only",
            action="store_true",
            help="Only sync properties",
        )
        parser.add_argument(
            "--stats-only",
            action="store_true",
            help="Only recalculate agent statistics",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Execute the command.

        Args:
            *args: Positional arguments.
            **options: Keyword arguments from command line.
        """
        full_sync = options["full"]
        year = options["year"] or timezone.now().year
        members_only = options["members_only"]
        properties_only = options["properties_only"]
        stats_only = options["stats_only"]

        self.stdout.write(
            self.style.NOTICE(
                f"Starting MLS sync at {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        )
        self.stdout.write(f"Mode: {'Full' if full_sync else 'Incremental'}")
        self.stdout.write(f"Year: {year}")

        try:
            if stats_only:
                # Only recalculate stats
                self.stdout.write("Recalculating agent statistics...")
                stats_count = calculate_agent_stats(year=year)
                self.stdout.write(
                    self.style.SUCCESS(f"Updated {stats_count} agent stats records")
                )
                return

            if full_sync and not (members_only or properties_only):
                # Full sync of everything
                self.stdout.write("Running full sync...")
                results = run_full_sync(year=year)

                for sync_type, sync_log in results.items():
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"{sync_type.capitalize()}: "
                            f"{sync_log.records_processed} processed, "
                            f"{sync_log.records_created} created, "
                            f"{sync_log.records_updated} updated"
                        )
                    )
                return

            # Selective sync
            if not properties_only:
                self.stdout.write("Syncing members...")
                member_log = sync_members(full_sync=full_sync)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Members: {member_log.records_processed} processed, "
                        f"{member_log.records_created} created, "
                        f"{member_log.records_updated} updated"
                    )
                )

            if not members_only:
                self.stdout.write("Syncing properties...")
                property_log = sync_properties(year=year, full_sync=full_sync)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Properties: {property_log.records_processed} processed, "
                        f"{property_log.records_created} created, "
                        f"{property_log.records_updated} updated"
                    )
                )

            # Always recalculate stats after sync
            self.stdout.write("Calculating agent statistics...")
            stats_count = calculate_agent_stats(year=year)
            self.stdout.write(
                self.style.SUCCESS(f"Updated {stats_count} agent stats records")
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Sync completed at {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Sync failed: {e}"))
            logger.exception("MLS sync failed")
            raise

