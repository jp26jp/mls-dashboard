"""Sync tasks for fetching MLS data.

This module contains functions for synchronizing member and property data
from the WFRMLS API using incremental updates based on modification timestamps.
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from wfrmls import MlsApi
from wfrmls.dataclasses import MemberData, PropertyData

from .models import AgentStats, Member, Property, SyncLog

logger = logging.getLogger(__name__)


def get_mls_api() -> MlsApi:
    """Get configured MLS API instance.

    Returns:
        Configured MlsApi instance.

    Raises:
        ValueError: If WFRMLS_BEARER_TOKEN is not configured.
    """
    token = settings.WFRMLS_BEARER_TOKEN
    if not token:
        raise ValueError("WFRMLS_BEARER_TOKEN not configured in settings")
    return MlsApi(bearer_token=token)


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
        wfrmls_api = get_mls_api()
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
        response = wfrmls_api.get_active_members()

        for r in response:
            try:
                member_data = MemberData(**r().data)
                records_processed += 1

                # Track the latest modification timestamp
                if member_data.ModificationTimestamp:
                    mod_ts = member_data.ModificationTimestamp
                    if isinstance(mod_ts, str):
                        mod_ts = datetime.fromisoformat(
                            mod_ts.replace("Z", "+00:00")
                        )
                    if sync_log.last_modification_timestamp is None:
                        sync_log.last_modification_timestamp = mod_ts
                    elif mod_ts > sync_log.last_modification_timestamp:
                        sync_log.last_modification_timestamp = mod_ts

                # Skip if no changes since last sync (incremental mode)
                if last_timestamp and member_data.ModificationTimestamp:
                    mod_ts = member_data.ModificationTimestamp
                    if isinstance(mod_ts, str):
                        mod_ts = datetime.fromisoformat(
                            mod_ts.replace("Z", "+00:00")
                        )
                    if mod_ts <= last_timestamp:
                        continue

                member, created = Member.objects.update_or_create(
                    member_key_numeric=member_data.MemberKeyNumeric,
                    defaults={
                        "office_key_numeric": member_data.OfficeKeyNumeric,
                        "member_aor_key": member_data.MemberAORkey,
                        "member_aor": member_data.MemberAOR,
                        "member_address1": member_data.MemberAddress1,
                        "member_address2": member_data.MemberAddress2,
                        "member_city": member_data.MemberCity,
                        "member_first_name": member_data.MemberFirstName,
                        "member_full_name": member_data.MemberFullName,
                        "member_key": member_data.MemberKey,
                        "member_last_name": member_data.MemberLastName,
                        "member_middle_name": member_data.MemberMiddleName,
                        "member_mls_id": member_data.MemberMlsId,
                        "member_mobile_phone": member_data.MemberMobilePhone,
                        "member_national_association_id": member_data.MemberNationalAssociationId,
                        "member_office_phone": member_data.MemberOfficePhone,
                        "member_postal_code": member_data.MemberPostalCode,
                        "member_preferred_phone": member_data.MemberPreferredPhone,
                        "member_state_license": member_data.MemberStateLicense,
                        "office_key": member_data.OfficeKey,
                        "office_mls_id": member_data.OfficeMlsId,
                        "office_name": member_data.OfficeName,
                        "originating_system_member_key": member_data.OriginatingSystemMemberKey,
                        "originating_system_name": member_data.OriginatingSystemName,
                        "member_mls_access_yn": member_data.MemberMlsAccessYN,
                        "modification_timestamp": member_data.ModificationTimestamp,
                        "original_entry_timestamp": member_data.OriginalEntryTimestamp,
                        "member_country": member_data.MemberCountry,
                        "member_county_or_parish": member_data.MemberCountyOrParish,
                        "member_state_license_state": member_data.MemberStateLicenseState,
                        "member_state_or_province": member_data.MemberStateOrProvince,
                        "member_status": member_data.MemberStatus,
                        "member_type": member_data.MemberType,
                        "member_designation": member_data.MemberDesignation,
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


def process_single_property(property_data: PropertyData) -> tuple[Property, bool]:
    """Process and save a single property.

    Args:
        property_data: PropertyData instance from the API.

    Returns:
        Tuple of (Property instance, was_created boolean).
    """
    property_obj, created = Property.objects.update_or_create(
        listing_key_numeric=property_data.ListingKeyNumeric,
        buyer_agent_key_numeric=property_data.BuyerAgentKeyNumeric,
        list_agent_key_numeric=property_data.ListAgentKeyNumeric,
        standard_status=property_data.StandardStatus,
        defaults={
            "association_fee": property_data.AssociationFee,
            "rooms_total": property_data.RoomsTotal,
            "stories": property_data.Stories,
            "bathrooms_full": property_data.BathroomsFull,
            "bathrooms_half": property_data.BathroomsHalf,
            "bathrooms_three_quarter": property_data.BathroomsThreeQuarter,
            "bathrooms_partial": property_data.BathroomsPartial,
            "bathrooms_total_integer": property_data.BathroomsTotalInteger,
            "bedrooms_total": property_data.BedroomsTotal,
            "buyer_office_key_numeric": property_data.BuyerOfficeKeyNumeric,
            "carport_spaces": property_data.CarportSpaces,
            "covered_spaces": property_data.CoveredSpaces,
            "close_price": property_data.ClosePrice,
            "co_list_agent_key_numeric": property_data.CoListAgentKeyNumeric,
            "co_list_office_key_numeric": property_data.CoListOfficeKeyNumeric,
            "concessions_amount": property_data.ConcessionsAmount,
            "cumulative_days_on_market": property_data.CumulativeDaysOnMarket,
            "days_on_market": property_data.DaysOnMarket,
            "fireplaces_total": property_data.FireplacesTotal,
            "garage_spaces": property_data.GarageSpaces,
            "list_office_key_numeric": property_data.ListOfficeKeyNumeric,
            "list_price": property_data.ListPrice,
            "lease_amount": property_data.LeaseAmount,
            "living_area": property_data.LivingArea,
            "building_area_total": property_data.BuildingAreaTotal,
            "lot_size_acres": property_data.LotSizeAcres,
            "lot_size_square_feet": property_data.LotSizeSquareFeet,
            "number_of_buildings": property_data.NumberOfBuildings,
            "number_of_units_leased": property_data.NumberOfUnitsLeased,
            "number_of_units_total": property_data.NumberOfUnitsTotal,
            "lot_size_area": property_data.LotSizeArea,
            "main_level_bedrooms": property_data.MainLevelBedrooms,
            "original_list_price": property_data.OriginalListPrice,
            "parking_total": property_data.ParkingTotal,
            "open_parking_spaces": property_data.OpenParkingSpaces,
            "photos_count": property_data.PhotosCount,
            "street_number_numeric": property_data.StreetNumberNumeric,
            "tax_annual_amount": property_data.TaxAnnualAmount,
            "year_built": property_data.YearBuilt,
            "year_built_effective": property_data.YearBuiltEffective,
            "mobile_length": property_data.MobileLength,
            "mobile_width": property_data.MobileWidth,
            "bathrooms_one_quarter": property_data.BathroomsOneQuarter,
            "cap_rate": property_data.CapRate,
            "number_of_pads": property_data.NumberOfPads,
            "stories_total": property_data.StoriesTotal,
            "year_established": property_data.YearEstablished,
            "association_name": property_data.AssociationName,
            "association_phone": property_data.AssociationPhone,
            "buyer_agent_fax": property_data.BuyerAgentFax,
            "buyer_agent_first_name": property_data.BuyerAgentFirstName,
            "buyer_agent_full_name": property_data.BuyerAgentFullName,
            "buyer_agent_key": property_data.BuyerAgentKey,
            "buyer_agent_last_name": property_data.BuyerAgentLastName,
            "buyer_agent_middle_name": property_data.BuyerAgentMiddleName,
            "buyer_agent_mls_id": property_data.BuyerAgentMlsId,
            "buyer_agent_office_phone": property_data.BuyerAgentOfficePhone,
            "buyer_agent_preferred_phone": property_data.BuyerAgentPreferredPhone,
            "buyer_agent_state_license": property_data.BuyerAgentStateLicense,
            "buyer_agent_url": property_data.BuyerAgentURL,
            "buyer_office_fax": property_data.BuyerOfficeFax,
            "buyer_office_key": property_data.BuyerOfficeKey,
            "buyer_office_mls_id": property_data.BuyerOfficeMlsId,
            "buyer_office_name": property_data.BuyerOfficeName,
            "buyer_office_phone": property_data.BuyerOfficePhone,
            "buyer_office_url": property_data.BuyerOfficeURL,
            "co_list_agent_fax": property_data.CoListAgentFax,
            "co_list_agent_first_name": property_data.CoListAgentFirstName,
            "co_list_agent_full_name": property_data.CoListAgentFullName,
            "co_list_agent_key": property_data.CoListAgentKey,
            "co_list_agent_last_name": property_data.CoListAgentLastName,
            "co_list_agent_middle_name": property_data.CoListAgentMiddleName,
            "co_list_agent_mls_id": property_data.CoListAgentMlsId,
            "co_list_agent_office_phone": property_data.CoListAgentOfficePhone,
            "co_list_agent_preferred_phone": property_data.CoListAgentPreferredPhone,
            "co_list_agent_state_license": property_data.CoListAgentStateLicense,
            "co_list_agent_url": property_data.CoListAgentURL,
            "co_list_office_fax": property_data.CoListOfficeFax,
            "co_list_office_key": property_data.CoListOfficeKey,
            "co_list_office_mls_id": property_data.CoListOfficeMlsId,
            "co_list_office_name": property_data.CoListOfficeName,
            "co_list_office_phone": property_data.CoListOfficePhone,
            "co_list_office_url": property_data.CoListOfficeURL,
            "copyright_notice": property_data.CopyrightNotice,
            "cross_street": property_data.CrossStreet,
            "directions": property_data.Directions,
            "disclaimer": property_data.Disclaimer,
            "exclusions": property_data.Exclusions,
            "frontage_length": property_data.FrontageLength,
            "inclusions": property_data.Inclusions,
            "list_agent_fax": property_data.ListAgentFax,
            "list_agent_first_name": property_data.ListAgentFirstName,
            "list_agent_full_name": property_data.ListAgentFullName,
            "list_agent_key": property_data.ListAgentKey,
            "list_agent_last_name": property_data.ListAgentLastName,
            "list_agent_middle_name": property_data.ListAgentMiddleName,
            "list_agent_mls_id": property_data.ListAgentMlsId,
            "list_agent_office_phone": property_data.ListAgentOfficePhone,
            "list_agent_preferred_phone": property_data.ListAgentPreferredPhone,
            "list_agent_state_license": property_data.ListAgentStateLicense,
            "list_agent_url": property_data.ListAgentURL,
            "list_office_fax": property_data.ListOfficeFax,
            "list_office_key": property_data.ListOfficeKey,
            "list_office_mls_id": property_data.ListOfficeMlsId,
            "list_office_name": property_data.ListOfficeName,
            "list_office_phone": property_data.ListOfficePhone,
            "list_office_url": property_data.ListOfficeURL,
            "listing_id": property_data.ListingId,
            "listing_key": property_data.ListingKey,
            "originating_system_id": property_data.OriginatingSystemID,
            "originating_system_key": property_data.OriginatingSystemKey,
            "originating_system_name": property_data.OriginatingSystemName,
            "other_parking": property_data.OtherParking,
            "ownership": property_data.Ownership,
            "parcel_number": property_data.ParcelNumber,
            "postal_code": property_data.PostalCode,
            "public_remarks": property_data.PublicRemarks,
            "rv_parking_dimensions": property_data.RVParkingDimensions,
            "showing_contact_name": property_data.ShowingContactName,
            "showing_contact_phone": property_data.ShowingContactPhone,
            "source_system_id": property_data.SourceSystemID,
            "source_system_key": property_data.SourceSystemKey,
            "source_system_name": property_data.SourceSystemName,
            "street_name": property_data.StreetName,
            "street_number": property_data.StreetNumber,
            "subdivision_name": property_data.SubdivisionName,
            "unit_number": property_data.UnitNumber,
            "unparsed_address": property_data.UnparsedAddress,
            "virtual_tour_url_branded": property_data.VirtualTourURLBranded,
            "virtual_tour_url_unbranded": property_data.VirtualTourURLUnbranded,
            "zoning": property_data.Zoning,
            "zoning_description": property_data.ZoningDescription,
            "lot_size_dimensions": property_data.LotSizeDimensions,
            "topography": property_data.Topography,
            "builder_name": property_data.BuilderName,
            "buyer_team_name": property_data.BuyerTeamName,
            "co_buyer_agent_first_name": property_data.CoBuyerAgentFirstName,
            "co_buyer_agent_full_name": property_data.CoBuyerAgentFullName,
            "co_buyer_agent_last_name": property_data.CoBuyerAgentLastName,
            "co_buyer_agent_state_license": property_data.CoBuyerAgentStateLicense,
            "co_buyer_office_mls_id": property_data.CoBuyerOfficeMlsId,
            "co_buyer_office_name": property_data.CoBuyerOfficeName,
            "doh1": property_data.DOH1,
            "doh2": property_data.DOH2,
            "doh3": property_data.DOH3,
            "license1": property_data.License1,
            "license2": property_data.License2,
            "license3": property_data.License3,
            "make": property_data.Make,
            "model": property_data.Model,
            "park_name": property_data.ParkName,
            "postal_code_plus4": property_data.PostalCodePlus4,
            "serial_u": property_data.SerialU,
            "serial_x": property_data.SerialX,
            "serial_xx": property_data.SerialXX,
            "street_additional_info": property_data.StreetAdditionalInfo,
            "street_suffix_modifier": property_data.StreetSuffixModifier,
            "water_body_name": property_data.WaterBodyName,
            "association_yn": property_data.AssociationYN,
            "attached_garage_yn": property_data.AttachedGarageYN,
            "carport_yn": property_data.CarportYN,
            "cooling_yn": property_data.CoolingYN,
            "fireplace_yn": property_data.FireplaceYN,
            "garage_yn": property_data.GarageYN,
            "heating_yn": property_data.HeatingYN,
            "home_warranty_yn": property_data.HomeWarrantyYN,
            "horse_yn": property_data.HorseYN,
            "internet_address_display_yn": property_data.InternetAddressDisplayYN,
            "searchable_yn": property_data.SearchableYN,
            "internet_entire_listing_display_yn": property_data.InternetEntireListingDisplayYN,
            "open_parking_yn": property_data.OpenParkingYN,
            "pool_private_yn": property_data.PoolPrivateYN,
            "senior_community_yn": property_data.SeniorCommunityYN,
            "spa_yn": property_data.SpaYN,
            "view_yn": property_data.ViewYN,
            "new_construction_yn": property_data.NewConstructionYN,
            "internet_automated_valuation_display_yn": property_data.InternetAutomatedValuationDisplayYN,
            "internet_consumer_comment_yn": property_data.InternetConsumerCommentYN,
            "lease_considered_yn": property_data.LeaseConsideredYN,
            "property_attached_yn": property_data.PropertyAttachedYN,
            "waterfront_yn": property_data.WaterfrontYN,
            "close_date": property_data.CloseDate,
            "contingent_date": property_data.ContingentDate,
            "contract_status_change_date": property_data.ContractStatusChangeDate,
            "listing_contract_date": property_data.ListingContractDate,
            "off_market_date": property_data.OffMarketDate,
            "on_market_date": property_data.OnMarketDate,
            "purchase_contract_date": property_data.PurchaseContractDate,
            "withdrawn_date": property_data.WithdrawnDate,
            "modification_timestamp": property_data.ModificationTimestamp,
            "original_entry_timestamp": property_data.OriginalEntryTimestamp,
            "photos_change_timestamp": property_data.PhotosChangeTimestamp,
            "price_change_timestamp": property_data.PriceChangeTimestamp,
            "status_change_timestamp": property_data.StatusChangeTimestamp,
            "association_fee_frequency": property_data.AssociationFeeFrequency,
            "buyer_agent_aor": property_data.BuyerAgentAOR,
            "city": property_data.City,
            "co_list_agent_aor": property_data.CoListAgentAOR,
            "co_list_office_aor": property_data.CoListOfficeAOR,
            "concessions": property_data.Concessions,
            "country": property_data.Country,
            "county_or_parish": property_data.CountyOrParish,
            "direction_faces": property_data.DirectionFaces,
            "elementary_school": property_data.ElementarySchool,
            "elementary_school_district": property_data.ElementarySchoolDistrict,
            "high_school": property_data.HighSchool,
            "high_school_district": property_data.HighSchoolDistrict,
            "list_agent_aor": property_data.ListAgentAOR,
            "list_office_aor": property_data.ListOfficeAOR,
            "listing_service": property_data.ListingService,
            "living_area_units": property_data.LivingAreaUnits,
            "lot_size_units": property_data.LotSizeUnits,
            "mls_area_major": property_data.MLSAreaMajor,
            "middle_or_junior_school": property_data.MiddleOrJuniorSchool,
            "middle_or_junior_school_district": property_data.MiddleOrJuniorSchoolDistrict,
            "mls_status": property_data.MlsStatus,
            "occupant_type": property_data.OccupantType,
            "postal_city": property_data.PostalCity,
            "property_sub_type": property_data.PropertySubType,
            "property_type": property_data.PropertyType,
            "state_or_province": property_data.StateOrProvince,
            "street_dir_prefix": property_data.StreetDirPrefix,
            "street_dir_suffix": property_data.StreetDirSuffix,
            "street_suffix": property_data.StreetSuffix,
            "lease_term": property_data.LeaseTerm,
            "living_area_source": property_data.LivingAreaSource,
            "year_built_source": property_data.YearBuiltSource,
            "accessibility_features": property_data.AccessibilityFeatures,
            "appliances": property_data.Appliances,
            "architectural_style": property_data.ArchitecturalStyle,
            "association_amenities": property_data.AssociationAmenities,
            "association_fee_includes": property_data.AssociationFeeIncludes,
            "basement": property_data.Basement,
            "buyer_agent_designation": property_data.BuyerAgentDesignation,
            "co_list_agent_designation": property_data.CoListAgentDesignation,
            "construction_materials": property_data.ConstructionMaterials,
            "cooling": property_data.Cooling,
            "door_features": property_data.DoorFeatures,
            "exterior_features": property_data.ExteriorFeatures,
            "flooring": property_data.Flooring,
            "green_building_verification_type": property_data.GreenBuildingVerificationType,
            "heating": property_data.Heating,
            "interior_features": property_data.InteriorFeatures,
            "laundry_features": property_data.LaundryFeatures,
            "list_agent_designation": property_data.ListAgentDesignation,
            "listing_terms": property_data.ListingTerms,
            "lot_features": property_data.LotFeatures,
            "other_equipment": property_data.OtherEquipment,
            "parking_features": property_data.ParkingFeatures,
            "patio_and_porch_features": property_data.PatioAndPorchFeatures,
            "pool_features": property_data.PoolFeatures,
            "property_condition": property_data.PropertyCondition,
            "roof": property_data.Roof,
            "security_features": property_data.SecurityFeatures,
            "sewer": property_data.Sewer,
            "showing_contact_type": property_data.ShowingContactType,
            "utilities": property_data.Utilities,
            "vegetation": property_data.Vegetation,
            "view": property_data.View,
            "water_source": property_data.WaterSource,
            "window_features": property_data.WindowFeatures,
            "current_use": property_data.CurrentUse,
            "fencing": property_data.Fencing,
            "fireplace_features": property_data.FireplaceFeatures,
            "green_energy_generation": property_data.GreenEnergyGeneration,
            "body_type": property_data.BodyType,
            "building_features": property_data.BuildingFeatures,
            "business_type": property_data.BusinessType,
            "common_walls": property_data.CommonWalls,
            "community_features": property_data.CommunityFeatures,
            "electric": property_data.Electric,
            "foundation_details": property_data.FoundationDetails,
            "green_energy_efficient": property_data.GreenEnergyEfficient,
            "green_indoor_air_quality": property_data.GreenIndoorAirQuality,
            "green_location": property_data.GreenLocation,
            "green_sustainability": property_data.GreenSustainability,
            "green_water_conservation": property_data.GreenWaterConservation,
            "levels": property_data.Levels,
            "other_structures": property_data.OtherStructures,
            "possible_use": property_data.PossibleUse,
            "rent_includes": property_data.RentIncludes,
            "road_frontage_type": property_data.RoadFrontageType,
            "road_surface_type": property_data.RoadSurfaceType,
            "room_type": property_data.RoomType,
            "skirt": property_data.Skirt,
            "spa_features": property_data.SpaFeatures,
            "special_listing_conditions": property_data.SpecialListingConditions,
            "structure_type": property_data.StructureType,
            "unit_type_type": property_data.UnitTypeType,
            "waterfront_features": property_data.WaterfrontFeatures,
            "geo_location": property_data.GeoLocation,
            "basement_finished": property_data.BasementFinished,
            "const_status": property_data.ConstStatus,
            "power_production_solar_year_install": property_data.PowerProductionSolarYearInstall,
            "solar_finance_company": property_data.SolarFinanceCompany,
            "solar_leasing_company": property_data.SolarLeasingCompany,
            "solar_ownership": property_data.SolarOwnership,
            "power_production_type": property_data.PowerProductionType,
            "level_data": property_data.LevelData,
            "above_grade_finished_area": property_data.AboveGradeFinishedArea,
            "buyer_financing": property_data.BuyerFinancing,
            "master_bedroom_level": property_data.MasterBedroomLevel,
            "irrigation_water_rights_acres": property_data.IrrigationWaterRightsAcres,
            "cancellation_date": property_data.CancellationDate,
            "image_status": property_data.ImageStatus,
            "co_buyer_agent_key_numeric": property_data.CoBuyerAgentKeyNumeric,
            "co_buyer_agent_fax": property_data.CoBuyerAgentFax,
            "co_buyer_agent_key": property_data.CoBuyerAgentKey,
            "co_buyer_agent_middle_name": property_data.CoBuyerAgentMiddleName,
            "co_buyer_agent_mls_id": property_data.CoBuyerAgentMlsId,
            "co_buyer_agent_preferred_phone": property_data.CoBuyerAgentPreferredPhone,
            "co_buyer_agent_url": property_data.CoBuyerAgentURL,
            "co_buyer_agent_aor": property_data.CoBuyerAgentAOR,
            "co_buyer_agent_designation": property_data.CoBuyerAgentDesignation,
            "co_buyer_office_key_numeric": property_data.CoBuyerOfficeKeyNumeric,
            "co_buyer_office_fax": property_data.CoBuyerOfficeFax,
            "co_buyer_office_key": property_data.CoBuyerOfficeKey,
            "co_buyer_office_phone": property_data.CoBuyerOfficePhone,
            "co_buyer_office_url": property_data.CoBuyerOfficeURL,
            "idx_contact_information": property_data.IdxContactInformation,
            "vow_contact_information": property_data.VowContactInformation,
            "short_term_rental_yn": property_data.ShortTermRentalYN,
            "adu_yn": property_data.AduYN,
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
        wfrmls_api = get_mls_api()
        records_processed = 0
        records_created = 0
        records_updated = 0

        # Build date range for the year
        closed_after = f"{year}-01-01"
        closed_before = f"{year}-12-31"

        logger.info(f"Fetching properties for {year}...")
        responses = wfrmls_api.get_properties(
            closed_after=closed_after,
            closed_before=closed_before,
        )

        # Collect all MLS IDs first
        mls_ids: list[str] = []
        for response in responses:
            mls_id = response().data.get("ListingKeyNumeric")
            if mls_id:
                mls_ids.append(mls_id)
            records_processed += 1
            if records_processed % 1000 == 0:
                logger.info(f"Collected {records_processed} property IDs...")

        logger.info(f"Collected {len(mls_ids)} property IDs, fetching details...")

        # Process each property
        for idx, mls_id in enumerate(mls_ids):
            try:
                property_data: PropertyData = wfrmls_api.get_property(mls_id=mls_id)

                # Track the latest modification timestamp
                if property_data.ModificationTimestamp:
                    mod_ts = property_data.ModificationTimestamp
                    if isinstance(mod_ts, str):
                        mod_ts = datetime.fromisoformat(mod_ts.replace("Z", "+00:00"))
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

                if (idx + 1) % 100 == 0:
                    logger.info(f"Processed {idx + 1}/{len(mls_ids)} properties...")

            except Exception as e:
                logger.error(f"Error processing property {mls_id}: {e}")
                continue

        sync_log.records_processed = len(mls_ids)
        sync_log.records_created = records_created
        sync_log.records_updated = records_updated
        sync_log.status = SyncLog.SyncStatus.COMPLETED
        sync_log.completed_at = timezone.now()
        sync_log.save()

        logger.info(
            f"Property sync completed: {len(mls_ids)} processed, "
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


def run_full_sync(year: Optional[int] = None) -> dict[str, SyncLog]:
    """Run a full synchronization of all MLS data.

    Args:
        year: Year to sync properties for. Defaults to current year.

    Returns:
        Dictionary with SyncLog instances for each sync type.
    """
    results = {}

    # Sync members
    logger.info("Starting member sync...")
    results["members"] = sync_members(full_sync=True)

    # Sync properties
    logger.info("Starting property sync...")
    results["properties"] = sync_properties(year=year, full_sync=True)

    # Calculate stats
    logger.info("Calculating agent stats...")
    calculate_agent_stats(year=year)

    return results

