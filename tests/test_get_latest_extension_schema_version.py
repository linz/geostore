from geostore.check_stac_metadata.stac_validators import get_latest_extension_schema_version


def should_get_latest_stac_spec_version() -> None:
    assert get_latest_extension_schema_version("stac-spec") == "1.0.0"
