"""Sync tasks for fetching MLS data.

This module contains functions for synchronizing member and property data
from the WFRMLS API using incremental updates based on modification timestamps.
"""

import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from wfrmls import WFRMLSClient
from wfrmls.exceptions import RateLimitError

from .models import AgentStats, Member, Property, SyncLog

logger = logging.getLogger(__name__)


def get_mls_client() -> WFRMLSClient:
    """Get configured WFRMLS client instance.

    Returns:
        Configured WFRMLSClient instance.

    Raises:
        ValueError: If WFRMLS_BEARER_TOKEN is not configured.
    """
    token = settings.WFRMLS_BEARER_TOKEN
    if not token:
        raise ValueError("WFRMLS_BEARER_TOKEN not configured in settings")
    return WFRMLSClient(bearer_token=token)


def sync_members(full_sync: bool = False) -> SyncLog:
    """Synchronize members from WFRMLS API.

    Args:
        full_sync: If True, fetch all members. If False, fetch only modified
            since last successful sync.

    Returns:
        SyncLog instance with sync results.
    """
    sync_log = SyncLog.objects.create(
        sync_type=SyncLog.SyncType.MEMBERS,
        status=SyncLog.SyncStatus.STARTED,
    )

    try:
        client = get_mls_client()
        records_processed = 0
        records_created = 0
        records_updated = 0
        last_timestamp: Optional[datetime] = None

        # Get last successful sync for incremental updates
        if not full_sync:
            last_sync = SyncLog.get_last_successful_sync(SyncLog.SyncType.MEMBERS)
            if last_sync and last_sync.last_modification_timestamp:
                last_timestamp = last_sync.last_modification_timestamp
                logger.info(f"Incremental sync from {last_timestamp}")

        logger.info("Fetching active members from WFRMLS...")

        # Use pagination to get all active members
        response = client.member.get_active_members(top=200)
        members_data = response.get("value", [])

        # Process initial batch and follow pagination
        while True:
            for member_data in members_data:
                try:
                    records_processed += 1

                    # Track the latest modification timestamp
                    mod_timestamp = member_data.get("ModificationTimestamp")
                    if mod_timestamp:
                        if isinstance(mod_timestamp, str):
                            mod_ts = datetime.fromisoformat(
                                mod_timestamp.replace("Z", "+00:00")
                            )
                        else:
                            mod_ts = mod_timestamp
                        if sync_log.last_modification_timestamp is None:
                            sync_log.last_modification_timestamp = mod_ts
                        elif mod_ts > sync_log.last_modification_timestamp:
                            sync_log.last_modification_timestamp = mod_ts

                        # Skip if no changes since last sync (incremental mode)
                        if last_timestamp and mod_ts <= last_timestamp:
                            continue

                    member_key_numeric = member_data.get("MemberKeyNumeric")
                    if not member_key_numeric:
                        continue

                    member, created = Member.objects.update_or_create(
                        member_key_numeric=member_key_numeric,
                        defaults={
                            "office_key_numeric": member_data.get("OfficeKeyNumeric"),
                            "member_aor_key": member_data.get("MemberAORkey"),
                            "member_aor": member_data.get("MemberAOR"),
                            "member_address1": member_data.get("MemberAddress1"),
                            "member_address2": member_data.get("MemberAddress2"),
                            "member_city": member_data.get("MemberCity"),
                            "member_first_name": member_data.get("MemberFirstName"),
                            "member_full_name": member_data.get("MemberFullName"),
                            "member_key": member_data.get("MemberKey"),
                            "member_last_name": member_data.get("MemberLastName"),
                            "member_middle_name": member_data.get("MemberMiddleName"),
                            "member_mls_id": member_data.get("MemberMlsId"),
                            "member_mobile_phone": member_data.get("MemberMobilePhone"),
                            "member_national_association_id": member_data.get(
                                "MemberNationalAssociationId"
                            ),
                            "member_office_phone": member_data.get("MemberOfficePhone"),
                            "member_postal_code": member_data.get("MemberPostalCode"),
                            "member_preferred_phone": member_data.get(
                                "MemberPreferredPhone"
                            ),
                            "member_state_license": member_data.get(
                                "MemberStateLicense"
                            ),
                            "office_key": member_data.get("OfficeKey"),
                            "office_mls_id": member_data.get("OfficeMlsId"),
                            "office_name": member_data.get("OfficeName"),
                            "originating_system_member_key": member_data.get(
                                "OriginatingSystemMemberKey"
                            ),
                            "originating_system_name": member_data.get(
                                "OriginatingSystemName"
                            ),
                            "member_mls_access_yn": member_data.get("MemberMlsAccessYN"),
                            "modification_timestamp": mod_timestamp,
                            "original_entry_timestamp": member_data.get(
                                "OriginalEntryTimestamp"
                            ),
                            "member_country": member_data.get("MemberCountry"),
                            "member_county_or_parish": member_data.get(
                                "MemberCountyOrParish"
                            ),
                            "member_state_license_state": member_data.get(
                                "MemberStateLicenseState"
                            ),
                            "member_state_or_province": member_data.get(
                                "MemberStateOrProvince"
                            ),
                            "member_status": member_data.get("MemberStatus"),
                            "member_type": member_data.get("MemberType"),
                            "member_designation": member_data.get("MemberDesignation"),
                        },
                    )

                    if created:
                        records_created += 1
                    else:
                        records_updated += 1

                    if records_processed % 500 == 0:
                        logger.info(f"Processed {records_processed} members...")

                except Exception as e:
                    logger.error(f"Error processing member: {e}")
                    continue

            # Check for next page
            next_link = response.get("@odata.nextLink")
            if not next_link:
                break

            # Extract the endpoint from the next link
            # The nextLink is a full URL, we need just the path + query
            if "?" in next_link:
                endpoint = (
                    next_link.split("/odata/")[1]
                    if "/odata/" in next_link
                    else next_link
                )
                # Add delay to avoid rate limiting
                time.sleep(1)
                try:
                    response = client.member.get(endpoint)
                except RateLimitError:
                    logger.warning("Rate limit hit, waiting 30 seconds...")
                    time.sleep(30)
                    response = client.member.get(endpoint)
                members_data = response.get("value", [])
            else:
                break

        sync_log.records_processed = records_processed
        sync_log.records_created = records_created
        sync_log.records_updated = records_updated
        sync_log.status = SyncLog.SyncStatus.COMPLETED
        sync_log.completed_at = timezone.now()
        sync_log.save()

        logger.info(
            f"Member sync completed: {records_processed} processed, "
            f"{records_created} created, {records_updated} updated"
        )

    except Exception as e:
        logger.error(f"Member sync failed: {e}")
        sync_log.status = SyncLog.SyncStatus.FAILED
        sync_log.error_message = str(e)
        sync_log.completed_at = timezone.now()
        sync_log.save()
        raise

    return sync_log


def process_single_property(property_data: dict[str, Any]) -> tuple[Property, bool]:
    """Process and save a single property.

    Args:
        property_data: Property data dictionary from the API.

    Returns:
        Tuple of (Property instance, was_created boolean).
    """
    property_obj, created = Property.objects.update_or_create(
        listing_key_numeric=property_data.get("ListingKeyNumeric"),
        buyer_agent_key_numeric=property_data.get("BuyerAgentKeyNumeric"),
        list_agent_key_numeric=property_data.get("ListAgentKeyNumeric"),
        standard_status=property_data.get("StandardStatus"),
        defaults={
            "association_fee": property_data.get("AssociationFee"),
            "rooms_total": property_data.get("RoomsTotal"),
            "stories": property_data.get("Stories"),
            "bathrooms_full": property_data.get("BathroomsFull"),
            "bathrooms_half": property_data.get("BathroomsHalf"),
            "bathrooms_three_quarter": property_data.get("BathroomsThreeQuarter"),
            "bathrooms_partial": property_data.get("BathroomsPartial"),
            "bathrooms_total_integer": property_data.get("BathroomsTotalInteger"),
            "bedrooms_total": property_data.get("BedroomsTotal"),
            "buyer_office_key_numeric": property_data.get("BuyerOfficeKeyNumeric"),
            "carport_spaces": property_data.get("CarportSpaces"),
            "covered_spaces": property_data.get("CoveredSpaces"),
            "close_price": property_data.get("ClosePrice"),
            "co_list_agent_key_numeric": property_data.get("CoListAgentKeyNumeric"),
            "co_list_office_key_numeric": property_data.get("CoListOfficeKeyNumeric"),
            "concessions_amount": property_data.get("ConcessionsAmount"),
            "cumulative_days_on_market": property_data.get("CumulativeDaysOnMarket"),
            "days_on_market": property_data.get("DaysOnMarket"),
            "fireplaces_total": property_data.get("FireplacesTotal"),
            "garage_spaces": property_data.get("GarageSpaces"),
            "list_office_key_numeric": property_data.get("ListOfficeKeyNumeric"),
            "list_price": property_data.get("ListPrice"),
            "lease_amount": property_data.get("LeaseAmount"),
            "living_area": property_data.get("LivingArea"),
            "building_area_total": property_data.get("BuildingAreaTotal"),
            "lot_size_acres": property_data.get("LotSizeAcres"),
            "lot_size_square_feet": property_data.get("LotSizeSquareFeet"),
            "number_of_buildings": property_data.get("NumberOfBuildings"),
            "number_of_units_leased": property_data.get("NumberOfUnitsLeased"),
            "number_of_units_total": property_data.get("NumberOfUnitsTotal"),
            "lot_size_area": property_data.get("LotSizeArea"),
            "main_level_bedrooms": property_data.get("MainLevelBedrooms"),
            "original_list_price": property_data.get("OriginalListPrice"),
            "parking_total": property_data.get("ParkingTotal"),
            "open_parking_spaces": property_data.get("OpenParkingSpaces"),
            "photos_count": property_data.get("PhotosCount"),
            "street_number_numeric": property_data.get("StreetNumberNumeric"),
            "tax_annual_amount": property_data.get("TaxAnnualAmount"),
            "year_built": property_data.get("YearBuilt"),
            "year_built_effective": property_data.get("YearBuiltEffective"),
            "mobile_length": property_data.get("MobileLength"),
            "mobile_width": property_data.get("MobileWidth"),
            "bathrooms_one_quarter": property_data.get("BathroomsOneQuarter"),
            "cap_rate": property_data.get("CapRate"),
            "number_of_pads": property_data.get("NumberOfPads"),
            "stories_total": property_data.get("StoriesTotal"),
            "year_established": property_data.get("YearEstablished"),
            "association_name": property_data.get("AssociationName"),
            "association_phone": property_data.get("AssociationPhone"),
            "buyer_agent_fax": property_data.get("BuyerAgentFax"),
            "buyer_agent_first_name": property_data.get("BuyerAgentFirstName"),
            "buyer_agent_full_name": property_data.get("BuyerAgentFullName"),
            "buyer_agent_key": property_data.get("BuyerAgentKey"),
            "buyer_agent_last_name": property_data.get("BuyerAgentLastName"),
            "buyer_agent_middle_name": property_data.get("BuyerAgentMiddleName"),
            "buyer_agent_mls_id": property_data.get("BuyerAgentMlsId"),
            "buyer_agent_office_phone": property_data.get("BuyerAgentOfficePhone"),
            "buyer_agent_preferred_phone": property_data.get("BuyerAgentPreferredPhone"),
            "buyer_agent_state_license": property_data.get("BuyerAgentStateLicense"),
            "buyer_agent_url": property_data.get("BuyerAgentURL"),
            "buyer_office_fax": property_data.get("BuyerOfficeFax"),
            "buyer_office_key": property_data.get("BuyerOfficeKey"),
            "buyer_office_mls_id": property_data.get("BuyerOfficeMlsId"),
            "buyer_office_name": property_data.get("BuyerOfficeName"),
            "buyer_office_phone": property_data.get("BuyerOfficePhone"),
            "buyer_office_url": property_data.get("BuyerOfficeURL"),
            "co_list_agent_fax": property_data.get("CoListAgentFax"),
            "co_list_agent_first_name": property_data.get("CoListAgentFirstName"),
            "co_list_agent_full_name": property_data.get("CoListAgentFullName"),
            "co_list_agent_key": property_data.get("CoListAgentKey"),
            "co_list_agent_last_name": property_data.get("CoListAgentLastName"),
            "co_list_agent_middle_name": property_data.get("CoListAgentMiddleName"),
            "co_list_agent_mls_id": property_data.get("CoListAgentMlsId"),
            "co_list_agent_office_phone": property_data.get("CoListAgentOfficePhone"),
            "co_list_agent_preferred_phone": property_data.get("CoListAgentPreferredPhone"),
            "co_list_agent_state_license": property_data.get("CoListAgentStateLicense"),
            "co_list_agent_url": property_data.get("CoListAgentURL"),
            "co_list_office_fax": property_data.get("CoListOfficeFax"),
            "co_list_office_key": property_data.get("CoListOfficeKey"),
            "co_list_office_mls_id": property_data.get("CoListOfficeMlsId"),
            "co_list_office_name": property_data.get("CoListOfficeName"),
            "co_list_office_phone": property_data.get("CoListOfficePhone"),
            "co_list_office_url": property_data.get("CoListOfficeURL"),
            "copyright_notice": property_data.get("CopyrightNotice"),
            "cross_street": property_data.get("CrossStreet"),
            "directions": property_data.get("Directions"),
            "disclaimer": property_data.get("Disclaimer"),
            "exclusions": property_data.get("Exclusions"),
            "frontage_length": property_data.get("FrontageLength"),
            "inclusions": property_data.get("Inclusions"),
            "list_agent_fax": property_data.get("ListAgentFax"),
            "list_agent_first_name": property_data.get("ListAgentFirstName"),
            "list_agent_full_name": property_data.get("ListAgentFullName"),
            "list_agent_key": property_data.get("ListAgentKey"),
            "list_agent_last_name": property_data.get("ListAgentLastName"),
            "list_agent_middle_name": property_data.get("ListAgentMiddleName"),
            "list_agent_mls_id": property_data.get("ListAgentMlsId"),
            "list_agent_office_phone": property_data.get("ListAgentOfficePhone"),
            "list_agent_preferred_phone": property_data.get("ListAgentPreferredPhone"),
            "list_agent_state_license": property_data.get("ListAgentStateLicense"),
            "list_agent_url": property_data.get("ListAgentURL"),
            "list_office_fax": property_data.get("ListOfficeFax"),
            "list_office_key": property_data.get("ListOfficeKey"),
            "list_office_mls_id": property_data.get("ListOfficeMlsId"),
            "list_office_name": property_data.get("ListOfficeName"),
            "list_office_phone": property_data.get("ListOfficePhone"),
            "list_office_url": property_data.get("ListOfficeURL"),
            "listing_id": property_data.get("ListingId"),
            "listing_key": property_data.get("ListingKey"),
            "originating_system_id": property_data.get("OriginatingSystemID"),
            "originating_system_key": property_data.get("OriginatingSystemKey"),
            "originating_system_name": property_data.get("OriginatingSystemName"),
            "other_parking": property_data.get("OtherParking"),
            "ownership": property_data.get("Ownership"),
            "parcel_number": property_data.get("ParcelNumber"),
            "postal_code": property_data.get("PostalCode"),
            "public_remarks": property_data.get("PublicRemarks"),
            "rv_parking_dimensions": property_data.get("RVParkingDimensions"),
            "showing_contact_name": property_data.get("ShowingContactName"),
            "showing_contact_phone": property_data.get("ShowingContactPhone"),
            "source_system_id": property_data.get("SourceSystemID"),
            "source_system_key": property_data.get("SourceSystemKey"),
            "source_system_name": property_data.get("SourceSystemName"),
            "street_name": property_data.get("StreetName"),
            "street_number": property_data.get("StreetNumber"),
            "subdivision_name": property_data.get("SubdivisionName"),
            "unit_number": property_data.get("UnitNumber"),
            "unparsed_address": property_data.get("UnparsedAddress"),
            "virtual_tour_url_branded": property_data.get("VirtualTourURLBranded"),
            "virtual_tour_url_unbranded": property_data.get("VirtualTourURLUnbranded"),
            "zoning": property_data.get("Zoning"),
            "zoning_description": property_data.get("ZoningDescription"),
            "lot_size_dimensions": property_data.get("LotSizeDimensions"),
            "topography": property_data.get("Topography"),
            "builder_name": property_data.get("BuilderName"),
            "buyer_team_name": property_data.get("BuyerTeamName"),
            "co_buyer_agent_first_name": property_data.get("CoBuyerAgentFirstName"),
            "co_buyer_agent_full_name": property_data.get("CoBuyerAgentFullName"),
            "co_buyer_agent_last_name": property_data.get("CoBuyerAgentLastName"),
            "co_buyer_agent_state_license": property_data.get("CoBuyerAgentStateLicense"),
            "co_buyer_office_mls_id": property_data.get("CoBuyerOfficeMlsId"),
            "co_buyer_office_name": property_data.get("CoBuyerOfficeName"),
            "doh1": property_data.get("DOH1"),
            "doh2": property_data.get("DOH2"),
            "doh3": property_data.get("DOH3"),
            "license1": property_data.get("License1"),
            "license2": property_data.get("License2"),
            "license3": property_data.get("License3"),
            "make": property_data.get("Make"),
            "model": property_data.get("Model"),
            "park_name": property_data.get("ParkName"),
            "postal_code_plus4": property_data.get("PostalCodePlus4"),
            "serial_u": property_data.get("SerialU"),
            "serial_x": property_data.get("SerialX"),
            "serial_xx": property_data.get("SerialXX"),
            "street_additional_info": property_data.get("StreetAdditionalInfo"),
            "street_suffix_modifier": property_data.get("StreetSuffixModifier"),
            "water_body_name": property_data.get("WaterBodyName"),
            "association_yn": property_data.get("AssociationYN"),
            "attached_garage_yn": property_data.get("AttachedGarageYN"),
            "carport_yn": property_data.get("CarportYN"),
            "cooling_yn": property_data.get("CoolingYN"),
            "fireplace_yn": property_data.get("FireplaceYN"),
            "garage_yn": property_data.get("GarageYN"),
            "heating_yn": property_data.get("HeatingYN"),
            "home_warranty_yn": property_data.get("HomeWarrantyYN"),
            "horse_yn": property_data.get("HorseYN"),
            "internet_address_display_yn": property_data.get("InternetAddressDisplayYN"),
            "searchable_yn": property_data.get("SearchableYN"),
            "internet_entire_listing_display_yn": property_data.get(
                "InternetEntireListingDisplayYN"
            ),
            "open_parking_yn": property_data.get("OpenParkingYN"),
            "pool_private_yn": property_data.get("PoolPrivateYN"),
            "senior_community_yn": property_data.get("SeniorCommunityYN"),
            "spa_yn": property_data.get("SpaYN"),
            "view_yn": property_data.get("ViewYN"),
            "new_construction_yn": property_data.get("NewConstructionYN"),
            "internet_automated_valuation_display_yn": property_data.get(
                "InternetAutomatedValuationDisplayYN"
            ),
            "internet_consumer_comment_yn": property_data.get("InternetConsumerCommentYN"),
            "lease_considered_yn": property_data.get("LeaseConsideredYN"),
            "property_attached_yn": property_data.get("PropertyAttachedYN"),
            "waterfront_yn": property_data.get("WaterfrontYN"),
            "close_date": property_data.get("CloseDate"),
            "contingent_date": property_data.get("ContingentDate"),
            "contract_status_change_date": property_data.get("ContractStatusChangeDate"),
            "listing_contract_date": property_data.get("ListingContractDate"),
            "off_market_date": property_data.get("OffMarketDate"),
            "on_market_date": property_data.get("OnMarketDate"),
            "purchase_contract_date": property_data.get("PurchaseContractDate"),
            "withdrawn_date": property_data.get("WithdrawnDate"),
            "modification_timestamp": property_data.get("ModificationTimestamp"),
            "original_entry_timestamp": property_data.get("OriginalEntryTimestamp"),
            "photos_change_timestamp": property_data.get("PhotosChangeTimestamp"),
            "price_change_timestamp": property_data.get("PriceChangeTimestamp"),
            "status_change_timestamp": property_data.get("StatusChangeTimestamp"),
            "association_fee_frequency": property_data.get("AssociationFeeFrequency"),
            "buyer_agent_aor": property_data.get("BuyerAgentAOR"),
            "city": property_data.get("City"),
            "co_list_agent_aor": property_data.get("CoListAgentAOR"),
            "co_list_office_aor": property_data.get("CoListOfficeAOR"),
            "concessions": property_data.get("Concessions"),
            "country": property_data.get("Country"),
            "county_or_parish": property_data.get("CountyOrParish"),
            "direction_faces": property_data.get("DirectionFaces"),
            "elementary_school": property_data.get("ElementarySchool"),
            "elementary_school_district": property_data.get("ElementarySchoolDistrict"),
            "high_school": property_data.get("HighSchool"),
            "high_school_district": property_data.get("HighSchoolDistrict"),
            "list_agent_aor": property_data.get("ListAgentAOR"),
            "list_office_aor": property_data.get("ListOfficeAOR"),
            "listing_service": property_data.get("ListingService"),
            "living_area_units": property_data.get("LivingAreaUnits"),
            "lot_size_units": property_data.get("LotSizeUnits"),
            "mls_area_major": property_data.get("MLSAreaMajor"),
            "middle_or_junior_school": property_data.get("MiddleOrJuniorSchool"),
            "middle_or_junior_school_district": property_data.get(
                "MiddleOrJuniorSchoolDistrict"
            ),
            "mls_status": property_data.get("MlsStatus"),
            "occupant_type": property_data.get("OccupantType"),
            "postal_city": property_data.get("PostalCity"),
            "property_sub_type": property_data.get("PropertySubType"),
            "property_type": property_data.get("PropertyType"),
            "state_or_province": property_data.get("StateOrProvince"),
            "street_dir_prefix": property_data.get("StreetDirPrefix"),
            "street_dir_suffix": property_data.get("StreetDirSuffix"),
            "street_suffix": property_data.get("StreetSuffix"),
            "lease_term": property_data.get("LeaseTerm"),
            "living_area_source": property_data.get("LivingAreaSource"),
            "year_built_source": property_data.get("YearBuiltSource"),
            "accessibility_features": property_data.get("AccessibilityFeatures"),
            "appliances": property_data.get("Appliances"),
            "architectural_style": property_data.get("ArchitecturalStyle"),
            "association_amenities": property_data.get("AssociationAmenities"),
            "association_fee_includes": property_data.get("AssociationFeeIncludes"),
            "basement": property_data.get("Basement"),
            "buyer_agent_designation": property_data.get("BuyerAgentDesignation"),
            "co_list_agent_designation": property_data.get("CoListAgentDesignation"),
            "construction_materials": property_data.get("ConstructionMaterials"),
            "cooling": property_data.get("Cooling"),
            "door_features": property_data.get("DoorFeatures"),
            "exterior_features": property_data.get("ExteriorFeatures"),
            "flooring": property_data.get("Flooring"),
            "green_building_verification_type": property_data.get(
                "GreenBuildingVerificationType"
            ),
            "heating": property_data.get("Heating"),
            "interior_features": property_data.get("InteriorFeatures"),
            "laundry_features": property_data.get("LaundryFeatures"),
            "list_agent_designation": property_data.get("ListAgentDesignation"),
            "listing_terms": property_data.get("ListingTerms"),
            "lot_features": property_data.get("LotFeatures"),
            "other_equipment": property_data.get("OtherEquipment"),
            "parking_features": property_data.get("ParkingFeatures"),
            "patio_and_porch_features": property_data.get("PatioAndPorchFeatures"),
            "pool_features": property_data.get("PoolFeatures"),
            "property_condition": property_data.get("PropertyCondition"),
            "roof": property_data.get("Roof"),
            "security_features": property_data.get("SecurityFeatures"),
            "sewer": property_data.get("Sewer"),
            "showing_contact_type": property_data.get("ShowingContactType"),
            "utilities": property_data.get("Utilities"),
            "vegetation": property_data.get("Vegetation"),
            "view": property_data.get("View"),
            "water_source": property_data.get("WaterSource"),
            "window_features": property_data.get("WindowFeatures"),
            "current_use": property_data.get("CurrentUse"),
            "fencing": property_data.get("Fencing"),
            "fireplace_features": property_data.get("FireplaceFeatures"),
            "green_energy_generation": property_data.get("GreenEnergyGeneration"),
            "body_type": property_data.get("BodyType"),
            "building_features": property_data.get("BuildingFeatures"),
            "business_type": property_data.get("BusinessType"),
            "common_walls": property_data.get("CommonWalls"),
            "community_features": property_data.get("CommunityFeatures"),
            "electric": property_data.get("Electric"),
            "foundation_details": property_data.get("FoundationDetails"),
            "green_energy_efficient": property_data.get("GreenEnergyEfficient"),
            "green_indoor_air_quality": property_data.get("GreenIndoorAirQuality"),
            "green_location": property_data.get("GreenLocation"),
            "green_sustainability": property_data.get("GreenSustainability"),
            "green_water_conservation": property_data.get("GreenWaterConservation"),
            "levels": property_data.get("Levels"),
            "other_structures": property_data.get("OtherStructures"),
            "possible_use": property_data.get("PossibleUse"),
            "rent_includes": property_data.get("RentIncludes"),
            "road_frontage_type": property_data.get("RoadFrontageType"),
            "road_surface_type": property_data.get("RoadSurfaceType"),
            "room_type": property_data.get("RoomType"),
            "skirt": property_data.get("Skirt"),
            "spa_features": property_data.get("SpaFeatures"),
            "special_listing_conditions": property_data.get("SpecialListingConditions"),
            "structure_type": property_data.get("StructureType"),
            "unit_type_type": property_data.get("UnitTypeType"),
            "waterfront_features": property_data.get("WaterfrontFeatures"),
            "geo_location": property_data.get("GeoLocation"),
            "basement_finished": property_data.get("BasementFinished"),
            "const_status": property_data.get("ConstStatus"),
            "power_production_solar_year_install": property_data.get(
                "PowerProductionSolarYearInstall"
            ),
            "solar_finance_company": property_data.get("SolarFinanceCompany"),
            "solar_leasing_company": property_data.get("SolarLeasingCompany"),
            "solar_ownership": property_data.get("SolarOwnership"),
            "power_production_type": property_data.get("PowerProductionType"),
            "level_data": property_data.get("LevelData"),
            "above_grade_finished_area": property_data.get("AboveGradeFinishedArea"),
            "buyer_financing": property_data.get("BuyerFinancing"),
            "master_bedroom_level": property_data.get("MasterBedroomLevel"),
            "irrigation_water_rights_acres": property_data.get("IrrigationWaterRightsAcres"),
            "cancellation_date": property_data.get("CancellationDate"),
            "image_status": property_data.get("ImageStatus"),
            "co_buyer_agent_key_numeric": property_data.get("CoBuyerAgentKeyNumeric"),
            "co_buyer_agent_fax": property_data.get("CoBuyerAgentFax"),
            "co_buyer_agent_key": property_data.get("CoBuyerAgentKey"),
            "co_buyer_agent_middle_name": property_data.get("CoBuyerAgentMiddleName"),
            "co_buyer_agent_mls_id": property_data.get("CoBuyerAgentMlsId"),
            "co_buyer_agent_preferred_phone": property_data.get("CoBuyerAgentPreferredPhone"),
            "co_buyer_agent_url": property_data.get("CoBuyerAgentURL"),
            "co_buyer_agent_aor": property_data.get("CoBuyerAgentAOR"),
            "co_buyer_agent_designation": property_data.get("CoBuyerAgentDesignation"),
            "co_buyer_office_key_numeric": property_data.get("CoBuyerOfficeKeyNumeric"),
            "co_buyer_office_fax": property_data.get("CoBuyerOfficeFax"),
            "co_buyer_office_key": property_data.get("CoBuyerOfficeKey"),
            "co_buyer_office_phone": property_data.get("CoBuyerOfficePhone"),
            "co_buyer_office_url": property_data.get("CoBuyerOfficeURL"),
            "idx_contact_information": property_data.get("IdxContactInformation"),
            "vow_contact_information": property_data.get("VowContactInformation"),
            "short_term_rental_yn": property_data.get("ShortTermRentalYN"),
            "adu_yn": property_data.get("AduYN"),
        },
    )
    return property_obj, created


def sync_properties(
    year: Optional[int] = None,
    full_sync: bool = False,
) -> SyncLog:
    """Synchronize properties from WFRMLS API.

    Args:
        year: Year to fetch properties for. Defaults to current year.
        full_sync: If True, fetch all properties for the year.
            If False, fetch only modified since last successful sync.

    Returns:
        SyncLog instance with sync results.
    """
    if year is None:
        year = timezone.now().year

    sync_log = SyncLog.objects.create(
        sync_type=SyncLog.SyncType.PROPERTIES,
        status=SyncLog.SyncStatus.STARTED,
    )

    try:
        client = get_mls_client()
        records_processed = 0
        records_created = 0
        records_updated = 0

        # Build filter for closed properties in the year
        filter_query = (
            f"StandardStatus eq 'Closed' and "
            f"CloseDate ge {year}-01-01 and CloseDate le {year}-12-31"
        )

        logger.info(f"Fetching closed properties for {year}...")

        # Process properties in pages to avoid memory issues
        page_num = 0
        while True:
            page_num += 1

            try:
                # Add delay between pages to avoid rate limiting
                if page_num > 1:
                    time.sleep(1)

                response = client.property.get_properties(
                    filter_query=filter_query,
                    top=200,
                    skip=(page_num - 1) * 200 if page_num > 1 else None,
                )
            except RateLimitError:
                logger.warning("Rate limit hit, waiting 30 seconds...")
                time.sleep(30)
                response = client.property.get_properties(
                    filter_query=filter_query,
                    top=200,
                    skip=(page_num - 1) * 200 if page_num > 1 else None,
                )

            properties_data = response.get("value", [])

            if not properties_data:
                break

            logger.info(f"Processing page {page_num} ({len(properties_data)} properties)")

            for property_data in properties_data:
                try:
                    records_processed += 1

                    # Track the latest modification timestamp
                    mod_timestamp = property_data.get("ModificationTimestamp")
                    if mod_timestamp:
                        if isinstance(mod_timestamp, str):
                            mod_ts = datetime.fromisoformat(
                                mod_timestamp.replace("Z", "+00:00")
                            )
                        else:
                            mod_ts = mod_timestamp
                        if sync_log.last_modification_timestamp is None:
                            sync_log.last_modification_timestamp = mod_ts
                        elif mod_ts > sync_log.last_modification_timestamp:
                            sync_log.last_modification_timestamp = mod_ts

                    with transaction.atomic():
                        _, created = process_single_property(property_data)

                    if created:
                        records_created += 1
                    else:
                        records_updated += 1

                    if records_processed % 100 == 0:
                        logger.info(f"Processed {records_processed} properties...")

                except Exception as e:
                    logger.error(f"Error processing property: {e}")
                    continue

            # Check if there are more pages (if we got fewer than 200, we're done)
            if len(properties_data) < 200:
                break

        sync_log.records_processed = records_processed
        sync_log.records_created = records_created
        sync_log.records_updated = records_updated
        sync_log.status = SyncLog.SyncStatus.COMPLETED
        sync_log.completed_at = timezone.now()
        sync_log.save()

        logger.info(
            f"Property sync completed: {records_processed} processed, "
            f"{records_created} created, {records_updated} updated"
        )

    except Exception as e:
        logger.error(f"Property sync failed: {e}")
        sync_log.status = SyncLog.SyncStatus.FAILED
        sync_log.error_message = str(e)
        sync_log.completed_at = timezone.now()
        sync_log.save()
        raise

    return sync_log


def calculate_agent_stats(year: Optional[int] = None) -> int:
    """Calculate and update agent statistics for rankings.

    Args:
        year: Year to calculate stats for. Defaults to current year.

    Returns:
        Number of agent stats records created/updated.
    """
    if year is None:
        year = timezone.now().year

    logger.info(f"Calculating agent stats for {year}...")

    # Get all closed properties for the year
    closed_properties = Property.objects.filter(
        standard_status="Closed",
        close_date__year=year,
        property_type="Residential",
    )

    # Calculate volumes for both listing and buying sides per AOR
    agent_volumes: dict[tuple[int, str], dict[str, Any]] = {}

    for prop in closed_properties:
        close_price = Decimal(str(prop.close_price)) if prop.close_price else Decimal("0")

        # Process listing agent
        if prop.list_agent_key_numeric:
            aor = prop.list_agent_aor or "Unknown"
            key = (prop.list_agent_key_numeric, aor)
            if key not in agent_volumes:
                agent_volumes[key] = {
                    "listing_volume": Decimal("0"),
                    "buyer_volume": Decimal("0"),
                    "listing_count": 0,
                    "buyer_count": 0,
                }
            agent_volumes[key]["listing_volume"] += close_price
            agent_volumes[key]["listing_count"] += 1

        # Process buyer agent
        if prop.buyer_agent_key_numeric:
            aor = prop.buyer_agent_aor or "Unknown"
            key = (prop.buyer_agent_key_numeric, aor)
            if key not in agent_volumes:
                agent_volumes[key] = {
                    "listing_volume": Decimal("0"),
                    "buyer_volume": Decimal("0"),
                    "listing_count": 0,
                    "buyer_count": 0,
                }
            agent_volumes[key]["buyer_volume"] += close_price
            agent_volumes[key]["buyer_count"] += 1

    # Create/update AgentStats records
    stats_updated = 0
    for (member_key, aor), volumes in agent_volumes.items():
        try:
            member = Member.objects.get(member_key_numeric=member_key)
            total_volume = volumes["listing_volume"] + volumes["buyer_volume"]
            total_count = volumes["listing_count"] + volumes["buyer_count"]
            avg_price = total_volume / total_count if total_count > 0 else None

            AgentStats.objects.update_or_create(
                member=member,
                year=year,
                aor=aor,
                defaults={
                    "total_volume": total_volume,
                    "listing_volume": volumes["listing_volume"],
                    "buyer_volume": volumes["buyer_volume"],
                    "transaction_count": total_count,
                    "listing_count": volumes["listing_count"],
                    "buyer_count": volumes["buyer_count"],
                    "average_price": avg_price,
                },
            )
            stats_updated += 1
        except Member.DoesNotExist:
            continue

    # Calculate rankings
    # Overall ranking
    all_stats = AgentStats.objects.filter(year=year).order_by("-total_volume")
    for rank, stat in enumerate(all_stats, 1):
        stat.rank_overall = rank
        stat.save(update_fields=["rank_overall"])

    # Per-AOR ranking
    aors = AgentStats.objects.filter(year=year).values_list("aor", flat=True).distinct()
    for aor in aors:
        aor_stats = AgentStats.objects.filter(year=year, aor=aor).order_by(
            "-total_volume"
        )
        for rank, stat in enumerate(aor_stats, 1):
            stat.rank_in_aor = rank
            stat.save(update_fields=["rank_in_aor"])

    logger.info(f"Updated {stats_updated} agent stats records")
    return stats_updated


def run_full_sync(year: Optional[int] = None) -> dict[str, Any]:
    """Run a full synchronization of all MLS data.

    Args:
        year: Year to sync properties for. Defaults to current year.

    Returns:
        Dictionary with SyncLog instances for each sync type.
    """
    results: dict[str, Any] = {}

    # Sync members
    logger.info("Starting member sync...")
    results["members"] = sync_members(full_sync=True)

    # Sync properties
    logger.info("Starting property sync...")
    results["properties"] = sync_properties(year=year, full_sync=True)

    # Calculate stats
    logger.info("Calculating agent stats...")
    results["stats_updated"] = calculate_agent_stats(year=year)

    return results

