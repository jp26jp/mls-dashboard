"""Models for MLS Sales Dashboard.

This module contains models for tracking MLS members (agents), properties,
sync logs for incremental updates, and computed agent statistics.
"""

from decimal import Decimal
from typing import Optional

from django.db import models
from django.db.models import Q, Sum
from django.utils import timezone


class Member(models.Model):
    """Represents an MLS member (real estate agent).

    Attributes:
        member_key_numeric: Unique numeric identifier for the member.
        member_full_name: Full name of the member.
        member_aor: Association of Realtors the member belongs to.
        office_name: Name of the member's brokerage office.
    """

    member_key_numeric = models.IntegerField(
        unique=True,
        help_text="Unique numeric identifier for the member",
    )
    office_key_numeric = models.IntegerField(null=True, blank=True)
    member_aor_key = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )
    member_aor = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        db_index=True,
        help_text="Association of Realtors",
    )

    member_first_name = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_middle_name = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_last_name = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_full_name = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        db_index=True,
    )

    member_address1 = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )
    member_address2 = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )
    member_city = models.CharField(
        max_length=100,
        blank=True,
        null=True,
    )
    member_key = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_mls_id = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_mobile_phone = models.CharField(
        max_length=20,
        blank=True,
        null=True,
    )

    member_national_association_id = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_office_phone = models.CharField(
        max_length=20,
        blank=True,
        null=True,
    )

    member_postal_code = models.CharField(
        max_length=10,
        blank=True,
        null=True,
    )

    member_preferred_phone = models.CharField(
        max_length=20,
        blank=True,
        null=True,
    )

    member_state_license = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    office_key = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    office_mls_id = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    office_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        db_index=True,
    )

    originating_system_member_key = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )

    originating_system_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )

    member_mls_access_yn = models.BooleanField(null=True, blank=True)

    modification_timestamp = models.DateTimeField(
        blank=True,
        null=True,
        db_index=True,
    )

    original_entry_timestamp = models.DateTimeField(
        blank=True,
        null=True,
    )

    member_country = models.CharField(
        max_length=100,
        blank=True,
        null=True,
    )

    member_county_or_parish = models.CharField(
        max_length=100,
        blank=True,
        null=True,
    )

    member_state_license_state = models.CharField(
        max_length=2,
        blank=True,
        null=True,
    )

    member_state_or_province = models.CharField(
        max_length=2,
        blank=True,
        null=True,
    )

    member_status = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_type = models.CharField(
        max_length=50,
        blank=True,
        null=True,
    )

    member_designation = models.TextField(
        blank=True,
        null=True,
    )

    class Meta:
        """Meta options for Member model."""

        verbose_name = "Member"
        verbose_name_plural = "Members"
        ordering = ["member_full_name"]

    def __str__(self) -> str:
        """Return string representation of the member.

        Returns:
            Full name of the member.
        """
        return f"{self.member_first_name} {self.member_last_name}"

    def get_total_volume(self, year: Optional[int] = None) -> Decimal:
        """Calculate total sales volume for this agent.

        Args:
            year: Optional year to filter by. Defaults to current year.

        Returns:
            Total volume as Decimal from both listing and buyer sides.
        """
        if year is None:
            year = timezone.now().year

        # Get properties where this agent was listing or buyer agent
        properties = Property.objects.filter(
            Q(list_agent_key_numeric=self.member_key_numeric)
            | Q(buyer_agent_key_numeric=self.member_key_numeric),
            standard_status="Closed",
            close_date__year=year,
        )

        total = Decimal("0.00")
        for prop in properties:
            if prop.close_price:
                total += Decimal(str(prop.close_price))

        return total

    def get_transaction_count(self, year: Optional[int] = None) -> int:
        """Get count of transactions for this agent.

        Args:
            year: Optional year to filter by. Defaults to current year.

        Returns:
            Number of closed transactions.
        """
        if year is None:
            year = timezone.now().year

        return Property.objects.filter(
            Q(list_agent_key_numeric=self.member_key_numeric)
            | Q(buyer_agent_key_numeric=self.member_key_numeric),
            standard_status="Closed",
            close_date__year=year,
        ).count()


class Property(models.Model):
    """Represents an MLS property listing.

    Attributes:
        listing_key_numeric: Unique numeric identifier for the listing.
        close_price: Final sale price when closed.
        standard_status: Current status (Active, Pending, Closed, etc.).
        close_date: Date the property closed/sold.
    """

    listing_key_numeric = models.IntegerField(null=True, blank=True, db_index=True)
    association_fee = models.FloatField(null=True, blank=True)
    rooms_total = models.IntegerField(null=True, blank=True)
    stories = models.IntegerField(null=True, blank=True)
    bathrooms_full = models.IntegerField(null=True, blank=True)
    bathrooms_half = models.IntegerField(null=True, blank=True)
    bathrooms_three_quarter = models.IntegerField(null=True, blank=True)
    bathrooms_partial = models.IntegerField(null=True, blank=True)
    bathrooms_total_integer = models.IntegerField(null=True, blank=True)
    bedrooms_total = models.IntegerField(null=True, blank=True)
    buyer_agent_key_numeric = models.IntegerField(null=True, blank=True, db_index=True)
    buyer_office_key_numeric = models.IntegerField(null=True, blank=True)
    carport_spaces = models.IntegerField(null=True, blank=True)
    covered_spaces = models.FloatField(null=True, blank=True)
    close_price = models.FloatField(null=True, blank=True, db_index=True)
    co_list_agent_key_numeric = models.IntegerField(null=True, blank=True)
    co_list_office_key_numeric = models.IntegerField(null=True, blank=True)
    concessions_amount = models.FloatField(null=True, blank=True)
    cumulative_days_on_market = models.IntegerField(null=True, blank=True)
    days_on_market = models.IntegerField(null=True, blank=True)
    fireplaces_total = models.IntegerField(null=True, blank=True)
    garage_spaces = models.FloatField(null=True, blank=True)
    list_agent_key_numeric = models.IntegerField(null=True, blank=True, db_index=True)
    list_office_key_numeric = models.IntegerField(null=True, blank=True)
    list_price = models.FloatField(null=True, blank=True)
    lease_amount = models.FloatField(null=True, blank=True)
    living_area = models.FloatField(null=True, blank=True)
    building_area_total = models.FloatField(null=True, blank=True)
    lot_size_acres = models.FloatField(null=True, blank=True)
    lot_size_square_feet = models.FloatField(null=True, blank=True)
    number_of_buildings = models.IntegerField(null=True, blank=True)
    number_of_units_leased = models.IntegerField(null=True, blank=True)
    number_of_units_total = models.IntegerField(null=True, blank=True)
    lot_size_area = models.FloatField(null=True, blank=True)
    main_level_bedrooms = models.IntegerField(null=True, blank=True)
    original_list_price = models.FloatField(null=True, blank=True)
    parking_total = models.FloatField(null=True, blank=True)
    open_parking_spaces = models.IntegerField(null=True, blank=True)
    photos_count = models.IntegerField(null=True, blank=True)
    street_number_numeric = models.IntegerField(null=True, blank=True)
    tax_annual_amount = models.FloatField(null=True, blank=True)
    year_built = models.IntegerField(null=True, blank=True)
    year_built_effective = models.IntegerField(null=True, blank=True)
    mobile_length = models.IntegerField(null=True, blank=True)
    mobile_width = models.IntegerField(null=True, blank=True)
    bathrooms_one_quarter = models.IntegerField(null=True, blank=True)
    cap_rate = models.FloatField(null=True, blank=True)
    number_of_pads = models.IntegerField(null=True, blank=True)
    stories_total = models.IntegerField(null=True, blank=True)
    year_established = models.IntegerField(null=True, blank=True)
    association_name = models.CharField(max_length=255, null=True, blank=True)
    association_phone = models.CharField(max_length=20, null=True, blank=True)
    buyer_agent_fax = models.CharField(max_length=20, null=True, blank=True)
    buyer_agent_first_name = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_full_name = models.CharField(max_length=255, null=True, blank=True)
    buyer_agent_key = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_last_name = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_middle_name = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_mls_id = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_office_phone = models.CharField(max_length=20, null=True, blank=True)
    buyer_agent_preferred_phone = models.CharField(max_length=20, null=True, blank=True)
    buyer_agent_state_license = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_url = models.URLField(null=True, blank=True)
    buyer_office_fax = models.CharField(max_length=20, null=True, blank=True)
    buyer_office_key = models.CharField(max_length=100, null=True, blank=True)
    buyer_office_mls_id = models.CharField(max_length=100, null=True, blank=True)
    buyer_office_name = models.CharField(max_length=255, null=True, blank=True)
    buyer_office_phone = models.CharField(max_length=20, null=True, blank=True)
    buyer_office_url = models.URLField(null=True, blank=True)
    co_list_agent_fax = models.CharField(max_length=20, null=True, blank=True)
    co_list_agent_first_name = models.CharField(max_length=100, null=True, blank=True)
    co_list_agent_full_name = models.CharField(max_length=255, null=True, blank=True)
    co_list_agent_key = models.CharField(max_length=100, null=True, blank=True)
    co_list_agent_last_name = models.CharField(max_length=100, null=True, blank=True)
    co_list_agent_middle_name = models.CharField(max_length=100, null=True, blank=True)
    co_list_agent_mls_id = models.CharField(max_length=100, null=True, blank=True)
    co_list_agent_office_phone = models.CharField(max_length=20, null=True, blank=True)
    co_list_agent_preferred_phone = models.CharField(
        max_length=20, null=True, blank=True
    )
    co_list_agent_state_license = models.CharField(
        max_length=100, null=True, blank=True
    )
    co_list_agent_url = models.URLField(null=True, blank=True)
    co_list_office_fax = models.CharField(max_length=20, null=True, blank=True)
    co_list_office_key = models.CharField(max_length=100, null=True, blank=True)
    co_list_office_mls_id = models.CharField(max_length=100, null=True, blank=True)
    co_list_office_name = models.CharField(max_length=255, null=True, blank=True)
    co_list_office_phone = models.CharField(max_length=20, null=True, blank=True)
    co_list_office_url = models.URLField(null=True, blank=True)
    copyright_notice = models.TextField(null=True, blank=True)
    cross_street = models.CharField(max_length=255, null=True, blank=True)
    directions = models.TextField(null=True, blank=True)
    disclaimer = models.TextField(null=True, blank=True)
    exclusions = models.TextField(null=True, blank=True)
    frontage_length = models.CharField(max_length=100, null=True, blank=True)
    inclusions = models.TextField(null=True, blank=True)
    list_agent_fax = models.CharField(max_length=20, null=True, blank=True)
    list_agent_first_name = models.CharField(max_length=100, null=True, blank=True)
    list_agent_full_name = models.CharField(max_length=255, null=True, blank=True)
    list_agent_key = models.CharField(max_length=100, null=True, blank=True)
    list_agent_last_name = models.CharField(max_length=100, null=True, blank=True)
    list_agent_middle_name = models.CharField(max_length=100, null=True, blank=True)
    list_agent_mls_id = models.CharField(max_length=100, null=True, blank=True)
    list_agent_office_phone = models.CharField(max_length=20, null=True, blank=True)
    list_agent_preferred_phone = models.CharField(max_length=20, null=True, blank=True)
    list_agent_state_license = models.CharField(max_length=100, null=True, blank=True)
    list_agent_url = models.URLField(null=True, blank=True)
    list_office_fax = models.CharField(max_length=20, null=True, blank=True)
    list_office_key = models.CharField(max_length=100, null=True, blank=True)
    list_office_mls_id = models.CharField(max_length=100, null=True, blank=True)
    list_office_name = models.CharField(max_length=255, null=True, blank=True)
    list_office_phone = models.CharField(max_length=20, null=True, blank=True)
    list_office_url = models.URLField(null=True, blank=True)
    listing_id = models.CharField(max_length=100, null=True, blank=True)
    listing_key = models.CharField(max_length=100, null=True, blank=True)
    originating_system_id = models.CharField(max_length=100, null=True, blank=True)
    originating_system_key = models.CharField(max_length=100, null=True, blank=True)
    originating_system_name = models.CharField(max_length=255, null=True, blank=True)
    other_parking = models.CharField(max_length=255, null=True, blank=True)
    ownership = models.CharField(max_length=100, null=True, blank=True)
    parcel_number = models.CharField(max_length=100, null=True, blank=True)
    postal_code = models.CharField(max_length=10, null=True, blank=True)
    public_remarks = models.TextField(null=True, blank=True)
    rv_parking_dimensions = models.CharField(max_length=100, null=True, blank=True)
    showing_contact_name = models.CharField(max_length=255, null=True, blank=True)
    showing_contact_phone = models.CharField(max_length=20, null=True, blank=True)
    source_system_id = models.CharField(max_length=100, null=True, blank=True)
    source_system_key = models.CharField(max_length=100, null=True, blank=True)
    source_system_name = models.CharField(max_length=255, null=True, blank=True)
    street_name = models.CharField(max_length=255, null=True, blank=True, db_index=True)
    street_number = models.CharField(max_length=20, null=True, blank=True)
    subdivision_name = models.CharField(max_length=255, null=True, blank=True)
    unit_number = models.CharField(max_length=20, null=True, blank=True)
    unparsed_address = models.CharField(max_length=255, null=True, blank=True)
    virtual_tour_url_branded = models.URLField(null=True, blank=True)
    virtual_tour_url_unbranded = models.URLField(null=True, blank=True)
    zoning = models.CharField(max_length=100, null=True, blank=True)
    zoning_description = models.TextField(null=True, blank=True)
    lot_size_dimensions = models.CharField(max_length=100, null=True, blank=True)
    topography = models.CharField(max_length=255, null=True, blank=True)
    builder_name = models.CharField(max_length=255, null=True, blank=True)
    buyer_team_name = models.CharField(max_length=255, null=True, blank=True)
    co_buyer_agent_first_name = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_agent_full_name = models.CharField(max_length=255, null=True, blank=True)
    co_buyer_agent_last_name = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_agent_state_license = models.CharField(
        max_length=100, null=True, blank=True
    )
    co_buyer_office_mls_id = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_office_name = models.CharField(max_length=255, null=True, blank=True)
    doh1 = models.CharField(max_length=100, null=True, blank=True)
    doh2 = models.CharField(max_length=100, null=True, blank=True)
    doh3 = models.CharField(max_length=100, null=True, blank=True)
    license1 = models.CharField(max_length=100, null=True, blank=True)
    license2 = models.CharField(max_length=100, null=True, blank=True)
    license3 = models.CharField(max_length=100, null=True, blank=True)
    make = models.CharField(max_length=100, null=True, blank=True)
    model = models.CharField(max_length=100, null=True, blank=True)
    park_name = models.CharField(max_length=255, null=True, blank=True)
    postal_code_plus4 = models.CharField(max_length=4, null=True, blank=True)
    serial_u = models.CharField(max_length=100, null=True, blank=True)
    serial_x = models.CharField(max_length=100, null=True, blank=True)
    serial_xx = models.CharField(max_length=100, null=True, blank=True)
    street_additional_info = models.CharField(max_length=255, null=True, blank=True)
    street_suffix_modifier = models.CharField(max_length=20, null=True, blank=True)
    water_body_name = models.CharField(max_length=255, null=True, blank=True)
    association_yn = models.BooleanField(null=True, blank=True)
    attached_garage_yn = models.BooleanField(null=True, blank=True)
    carport_yn = models.BooleanField(null=True, blank=True)
    cooling_yn = models.BooleanField(null=True, blank=True)
    fireplace_yn = models.BooleanField(null=True, blank=True)
    garage_yn = models.BooleanField(null=True, blank=True)
    heating_yn = models.BooleanField(null=True, blank=True)
    home_warranty_yn = models.BooleanField(null=True, blank=True)
    horse_yn = models.BooleanField(null=True, blank=True)
    internet_address_display_yn = models.BooleanField(null=True, blank=True)
    searchable_yn = models.BooleanField(null=True, blank=True)
    internet_entire_listing_display_yn = models.BooleanField(null=True, blank=True)
    open_parking_yn = models.BooleanField(null=True, blank=True)
    pool_private_yn = models.BooleanField(null=True, blank=True)
    senior_community_yn = models.BooleanField(null=True, blank=True)
    spa_yn = models.BooleanField(null=True, blank=True)
    view_yn = models.BooleanField(null=True, blank=True)
    new_construction_yn = models.BooleanField(null=True, blank=True)
    internet_automated_valuation_display_yn = models.BooleanField(null=True, blank=True)
    internet_consumer_comment_yn = models.BooleanField(null=True, blank=True)
    lease_considered_yn = models.BooleanField(null=True, blank=True)
    property_attached_yn = models.BooleanField(null=True, blank=True)
    waterfront_yn = models.BooleanField(null=True, blank=True)
    close_date = models.DateField(null=True, blank=True, db_index=True)
    contingent_date = models.DateField(null=True, blank=True)
    contract_status_change_date = models.DateField(null=True, blank=True)
    listing_contract_date = models.DateField(null=True, blank=True)
    off_market_date = models.DateField(null=True, blank=True)
    on_market_date = models.DateField(null=True, blank=True)
    purchase_contract_date = models.DateField(null=True, blank=True)
    withdrawn_date = models.DateField(null=True, blank=True)
    modification_timestamp = models.DateTimeField(null=True, blank=True, db_index=True)
    original_entry_timestamp = models.DateTimeField(null=True, blank=True)
    photos_change_timestamp = models.DateTimeField(null=True, blank=True)
    price_change_timestamp = models.DateTimeField(null=True, blank=True)
    status_change_timestamp = models.DateTimeField(null=True, blank=True)
    association_fee_frequency = models.CharField(max_length=100, null=True, blank=True)
    buyer_agent_aor = models.CharField(
        max_length=255, null=True, blank=True, db_index=True
    )
    city = models.CharField(max_length=100, null=True, blank=True, db_index=True)
    co_list_agent_aor = models.CharField(max_length=255, null=True, blank=True)
    co_list_office_aor = models.CharField(max_length=255, null=True, blank=True)
    concessions = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    county_or_parish = models.CharField(max_length=100, null=True, blank=True)
    direction_faces = models.CharField(max_length=100, null=True, blank=True)
    elementary_school = models.CharField(max_length=255, null=True, blank=True)
    elementary_school_district = models.CharField(max_length=255, null=True, blank=True)
    high_school = models.CharField(max_length=255, null=True, blank=True)
    high_school_district = models.CharField(max_length=255, null=True, blank=True)
    list_agent_aor = models.CharField(
        max_length=255, null=True, blank=True, db_index=True
    )
    list_office_aor = models.CharField(max_length=255, null=True, blank=True)
    listing_service = models.CharField(max_length=100, null=True, blank=True)
    living_area_units = models.CharField(max_length=100, null=True, blank=True)
    lot_size_units = models.CharField(max_length=100, null=True, blank=True)
    mls_area_major = models.CharField(max_length=100, null=True, blank=True)
    middle_or_junior_school = models.CharField(max_length=255, null=True, blank=True)
    middle_or_junior_school_district = models.CharField(
        max_length=255, null=True, blank=True
    )
    mls_status = models.CharField(max_length=100, null=True, blank=True)
    occupant_type = models.CharField(max_length=100, null=True, blank=True)
    postal_city = models.CharField(max_length=100, null=True, blank=True)
    property_sub_type = models.CharField(max_length=100, null=True, blank=True)
    property_type = models.CharField(
        max_length=100, null=True, blank=True, db_index=True
    )
    standard_status = models.CharField(
        max_length=100, null=True, blank=True, db_index=True
    )
    state_or_province = models.CharField(max_length=100, null=True, blank=True)
    street_dir_prefix = models.CharField(max_length=20, null=True, blank=True)
    street_dir_suffix = models.CharField(max_length=20, null=True, blank=True)
    street_suffix = models.CharField(max_length=20, null=True, blank=True)
    lease_term = models.CharField(max_length=100, null=True, blank=True)
    living_area_source = models.CharField(max_length=100, null=True, blank=True)
    year_built_source = models.CharField(max_length=100, null=True, blank=True)
    accessibility_features = models.TextField(null=True, blank=True)
    appliances = models.TextField(null=True, blank=True)
    architectural_style = models.TextField(null=True, blank=True)
    association_amenities = models.TextField(null=True, blank=True)
    association_fee_includes = models.TextField(null=True, blank=True)
    basement = models.TextField(null=True, blank=True)
    buyer_agent_designation = models.TextField(null=True, blank=True)
    co_list_agent_designation = models.TextField(null=True, blank=True)
    construction_materials = models.TextField(null=True, blank=True)
    cooling = models.TextField(null=True, blank=True)
    door_features = models.TextField(null=True, blank=True)
    exterior_features = models.TextField(null=True, blank=True)
    flooring = models.TextField(null=True, blank=True)
    green_building_verification_type = models.TextField(null=True, blank=True)
    heating = models.TextField(null=True, blank=True)
    interior_features = models.TextField(null=True, blank=True)
    laundry_features = models.TextField(null=True, blank=True)
    list_agent_designation = models.TextField(null=True, blank=True)
    listing_terms = models.TextField(null=True, blank=True)
    lot_features = models.TextField(null=True, blank=True)
    other_equipment = models.TextField(null=True, blank=True)
    parking_features = models.TextField(null=True, blank=True)
    patio_and_porch_features = models.TextField(null=True, blank=True)
    pool_features = models.TextField(null=True, blank=True)
    property_condition = models.TextField(null=True, blank=True)
    roof = models.TextField(null=True, blank=True)
    security_features = models.TextField(null=True, blank=True)
    sewer = models.TextField(null=True, blank=True)
    showing_contact_type = models.CharField(max_length=100, null=True, blank=True)
    utilities = models.TextField(null=True, blank=True)
    vegetation = models.TextField(null=True, blank=True)
    view = models.TextField(null=True, blank=True)
    water_source = models.TextField(null=True, blank=True)
    window_features = models.TextField(null=True, blank=True)
    current_use = models.TextField(null=True, blank=True)
    fencing = models.TextField(null=True, blank=True)
    fireplace_features = models.TextField(null=True, blank=True)
    green_energy_generation = models.TextField(null=True, blank=True)
    body_type = models.CharField(max_length=100, null=True, blank=True)
    building_features = models.TextField(null=True, blank=True)
    business_type = models.CharField(max_length=100, null=True, blank=True)
    common_walls = models.TextField(null=True, blank=True)
    community_features = models.TextField(null=True, blank=True)
    electric = models.TextField(null=True, blank=True)
    foundation_details = models.TextField(null=True, blank=True)
    green_energy_efficient = models.TextField(null=True, blank=True)
    green_indoor_air_quality = models.TextField(null=True, blank=True)
    green_location = models.TextField(null=True, blank=True)
    green_sustainability = models.TextField(null=True, blank=True)
    green_water_conservation = models.TextField(null=True, blank=True)
    levels = models.TextField(null=True, blank=True)
    other_structures = models.TextField(null=True, blank=True)
    possible_use = models.TextField(null=True, blank=True)
    rent_includes = models.TextField(null=True, blank=True)
    road_frontage_type = models.TextField(null=True, blank=True)
    road_surface_type = models.TextField(null=True, blank=True)
    room_type = models.TextField(null=True, blank=True)
    skirt = models.CharField(max_length=100, null=True, blank=True)
    spa_features = models.TextField(null=True, blank=True)
    special_listing_conditions = models.TextField(null=True, blank=True)
    structure_type = models.CharField(max_length=100, null=True, blank=True)
    unit_type_type = models.CharField(max_length=100, null=True, blank=True)
    waterfront_features = models.TextField(null=True, blank=True)
    geo_location = models.CharField(max_length=255, null=True, blank=True)
    basement_finished = models.IntegerField(null=True, blank=True)
    const_status = models.CharField(max_length=100, null=True, blank=True)
    power_production_solar_year_install = models.CharField(
        max_length=4, null=True, blank=True
    )
    solar_finance_company = models.CharField(max_length=255, null=True, blank=True)
    solar_leasing_company = models.CharField(max_length=255, null=True, blank=True)
    solar_ownership = models.CharField(max_length=100, null=True, blank=True)
    power_production_type = models.CharField(max_length=100, null=True, blank=True)
    level_data = models.TextField(null=True, blank=True)
    above_grade_finished_area = models.FloatField(null=True, blank=True)
    buyer_financing = models.CharField(max_length=100, null=True, blank=True)
    master_bedroom_level = models.CharField(max_length=100, null=True, blank=True)
    irrigation_water_rights_acres = models.IntegerField(null=True, blank=True)
    cancellation_date = models.DateField(null=True, blank=True)
    image_status = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_agent_key_numeric = models.IntegerField(null=True, blank=True)
    co_buyer_agent_fax = models.CharField(max_length=20, null=True, blank=True)
    co_buyer_agent_key = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_agent_middle_name = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_agent_mls_id = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_agent_preferred_phone = models.CharField(
        max_length=20, null=True, blank=True
    )
    co_buyer_agent_url = models.URLField(null=True, blank=True)
    co_buyer_agent_aor = models.CharField(max_length=255, null=True, blank=True)
    co_buyer_agent_designation = models.TextField(null=True, blank=True)
    co_buyer_office_key_numeric = models.IntegerField(null=True, blank=True)
    co_buyer_office_fax = models.CharField(max_length=20, null=True, blank=True)
    co_buyer_office_key = models.CharField(max_length=100, null=True, blank=True)
    co_buyer_office_phone = models.CharField(max_length=20, null=True, blank=True)
    co_buyer_office_url = models.URLField(null=True, blank=True)
    idx_contact_information = models.TextField(null=True, blank=True)
    vow_contact_information = models.TextField(null=True, blank=True)
    short_term_rental_yn = models.BooleanField(null=True, blank=True)
    adu_yn = models.BooleanField(null=True, blank=True)

    class Meta:
        """Meta options for Property model."""

        verbose_name = "Property"
        verbose_name_plural = "Properties"
        unique_together = [
            "listing_key_numeric",
            "buyer_agent_key_numeric",
            "list_agent_key_numeric",
            "standard_status",
        ]

    def __str__(self) -> str:
        """Return string representation of the property.

        Returns:
            Address string combining street number, name, city, and state.
        """
        return f"{self.street_number} {self.street_name}, {self.city}, {self.state_or_province}"

    @property
    def full_address(self) -> str:
        """Get the full formatted address.

        Returns:
            Complete address string.
        """
        parts = [
            self.street_number,
            self.street_name,
        ]
        address_line = " ".join(filter(None, parts))
        city_state = f"{self.city}, {self.state_or_province}" if self.city else ""
        postal = self.postal_code or ""
        return f"{address_line}, {city_state} {postal}".strip()


class SyncLog(models.Model):
    """Tracks synchronization history for incremental MLS data updates.

    Attributes:
        sync_type: Type of sync (members or properties).
        started_at: When the sync started.
        completed_at: When the sync completed.
        records_processed: Number of records processed.
        status: Current status of the sync.
    """

    class SyncType(models.TextChoices):
        """Types of synchronization operations."""

        MEMBERS = "members", "Members"
        PROPERTIES = "properties", "Properties"
        FULL = "full", "Full Sync"

    class SyncStatus(models.TextChoices):
        """Status of synchronization operations."""

        STARTED = "started", "Started"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"

    sync_type = models.CharField(
        max_length=20,
        choices=SyncType.choices,
        db_index=True,
    )
    started_at = models.DateTimeField(
        default=timezone.now,
        db_index=True,
    )
    completed_at = models.DateTimeField(
        null=True,
        blank=True,
    )
    records_processed = models.IntegerField(default=0)
    records_created = models.IntegerField(default=0)
    records_updated = models.IntegerField(default=0)
    status = models.CharField(
        max_length=20,
        choices=SyncStatus.choices,
        default=SyncStatus.STARTED,
    )
    error_message = models.TextField(blank=True, null=True)
    last_modification_timestamp = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last modification timestamp from MLS data for incremental sync",
    )

    class Meta:
        """Meta options for SyncLog model."""

        verbose_name = "Sync Log"
        verbose_name_plural = "Sync Logs"
        ordering = ["-started_at"]
        get_latest_by = "started_at"

    def __str__(self) -> str:
        """Return string representation of the sync log.

        Returns:
            String with sync type, status, and timestamp.
        """
        return f"{self.get_sync_type_display()} - {self.get_status_display()} - {self.started_at}"

    @classmethod
    def get_last_successful_sync(
        cls, sync_type: str
    ) -> Optional["SyncLog"]:
        """Get the last successful sync of a given type.

        Args:
            sync_type: The type of sync to look for.

        Returns:
            The last successful SyncLog or None if no successful syncs exist.
        """
        return (
            cls.objects.filter(
                sync_type=sync_type,
                status=cls.SyncStatus.COMPLETED,
            )
            .order_by("-completed_at")
            .first()
        )


class AgentStats(models.Model):
    """Cached statistics for agent performance rankings.

    Pre-computed statistics to avoid expensive queries on every page load.

    Attributes:
        member: The member these stats belong to.
        year: The year these stats are for.
        total_volume: Total sales volume.
        transaction_count: Number of transactions.
        rank_overall: Overall rank by volume.
    """

    member = models.ForeignKey(
        Member,
        on_delete=models.CASCADE,
        related_name="stats",
    )
    year = models.IntegerField(db_index=True)
    aor = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        db_index=True,
        help_text="Association of Realtors for this stat",
    )
    total_volume = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        default=Decimal("0.00"),
        db_index=True,
    )
    listing_volume = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        default=Decimal("0.00"),
    )
    buyer_volume = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        default=Decimal("0.00"),
    )
    transaction_count = models.IntegerField(default=0)
    listing_count = models.IntegerField(default=0)
    buyer_count = models.IntegerField(default=0)
    rank_overall = models.IntegerField(
        null=True,
        blank=True,
        db_index=True,
    )
    rank_in_aor = models.IntegerField(
        null=True,
        blank=True,
        db_index=True,
    )
    average_price = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        """Meta options for AgentStats model."""

        verbose_name = "Agent Stats"
        verbose_name_plural = "Agent Stats"
        unique_together = ["member", "year", "aor"]
        ordering = ["-total_volume"]

    def __str__(self) -> str:
        """Return string representation of the agent stats.

        Returns:
            String with member name, year, and volume.
        """
        return f"{self.member.member_full_name} - {self.year} - ${self.total_volume:,.2f}"

