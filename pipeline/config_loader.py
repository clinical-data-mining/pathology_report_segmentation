"""Configuration loader for Databricks settings - Pathology Report Segmentation Pipeline.

This module provides utilities to load and parse YAML configuration files
for Databricks connections and table configurations specific to the pathology pipeline.
"""

import os
from typing import Dict, Any, Union

import yaml

from databricks_io import DatabricksTableConfig


def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration from the specified path.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Dictionary containing the parsed configuration

    Raises:
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the config file is not valid YAML
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    return config


def get_volume_root(config: Dict[str, Any]) -> str:
    """Construct the volume root path from configuration.

    Args:
        config: Configuration dictionary

    Returns:
        Full volume root path (e.g., '/Volumes/catalog/schema/volume')
    """
    catalog = config['databricks']['destination']['catalog']
    schema = config['databricks']['destination']['schema']
    volume = config['databricks']['destination']['volume']

    return os.path.join("/Volumes", catalog, schema, volume)


def get_external_source_table(config: Dict[str, Any], source_key: str) -> str:
    """Get the fully qualified name for an external source table.

    Args:
        config: Configuration dictionary
        source_key: Key in 'external_sources' (e.g., 'id_mapping', 'pathology_reports')

    Returns:
        Fully qualified table name (e.g., 'catalog.schema.table')

    Raises:
        KeyError: If the source_key is not found in configuration
    """
    if source_key not in config['external_sources']:
        raise KeyError(f"External source '{source_key}' not found in configuration")

    source = config['external_sources'][source_key]
    return f"{source['catalog']}.{source['schema']}.{source['table']}"


def get_legacy_table(config: Dict[str, Any], legacy_key: str) -> str:
    """Get the fully qualified name for a legacy IDB table.

    Args:
        config: Configuration dictionary
        legacy_key: Key in 'legacy_idb' (e.g., 'dop_summary', 'pdl1_calls')

    Returns:
        Fully qualified table name (e.g., 'catalog.schema.table')

    Raises:
        KeyError: If the legacy_key is not found in configuration
    """
    if legacy_key not in config['legacy_idb']:
        raise KeyError(f"Legacy source '{legacy_key}' not found in configuration")

    legacy = config['legacy_idb'][legacy_key]
    return f"{legacy['catalog']}.{legacy['schema']}.{legacy['table']}"


def get_step1_table(config: Dict[str, Any], table_key: str) -> str:
    """Get the fully qualified table name for a Step 1 extraction output.

    Args:
        config: Configuration dictionary
        table_key: Key in 'step1_extraction' (e.g., 'path_accessions', 'pathology_pdl1_calls_epic')

    Returns:
        Fully qualified table name (e.g., 'catalog.schema.table')

    Raises:
        KeyError: If the table_key is not found in configuration
    """
    if table_key not in config['step1_extraction']:
        raise KeyError(f"Step 1 table '{table_key}' not found in configuration")

    catalog = config['databricks']['destination']['catalog']
    schema = config['databricks']['destination']['schema']
    table = config['step1_extraction'][table_key]['output_table']['table']

    return f"{catalog}.{schema}.{table}"


def get_step2_table(config: Dict[str, Any], table_key: str) -> str:
    """Get the fully qualified table name for a Step 2 combining output.

    Args:
        config: Configuration dictionary
        table_key: Key in 'step2_combining' (e.g., 'pathology_pdl1_calls_epic_idb_combined')

    Returns:
        Fully qualified table name (e.g., 'catalog.schema.table')

    Raises:
        KeyError: If the table_key is not found in configuration
    """
    if table_key not in config['step2_combining']:
        raise KeyError(f"Step 2 table '{table_key}' not found in configuration")

    catalog = config['databricks']['destination']['catalog']
    schema = config['databricks']['destination']['schema']
    table = config['step2_combining'][table_key]['output_table']['table']

    return f"{catalog}.{schema}.{table}"


def get_step3_table(config: Dict[str, Any], table_key: str) -> str:
    """Get the fully qualified table name for a Step 3 cBioPortal output.

    Args:
        config: Configuration dictionary
        table_key: Key in 'step3_cbioportal' (e.g., 'timeline_pdl1_calls', 'summary_pdl1_patient')

    Returns:
        Fully qualified table name (e.g., 'catalog.schema.table')

    Raises:
        KeyError: If the table_key is not found in configuration
    """
    if table_key not in config['step3_cbioportal']:
        raise KeyError(f"Step 3 table '{table_key}' not found in configuration")

    catalog = config['databricks']['destination']['catalog']
    schema = config['databricks']['destination']['schema']
    table = config['step3_cbioportal'][table_key]['output_table']['table']

    return f"{catalog}.{schema}.{table}"


def get_output_table_config(config: Dict[str, Any], step: str, table_key: str) -> DatabricksTableConfig:
    """Get DatabricksTableConfig object for an output table.

    Args:
        config: Configuration dictionary
        step: Pipeline step ('step1_extraction', 'step2_combining', or 'step3_cbioportal')
        table_key: Key of the output table in config

    Returns:
        DatabricksTableConfig object with all settings

    Raises:
        KeyError: If the step or table_key is not found in configuration
    """
    if step not in config:
        raise KeyError(f"Step '{step}' not found in configuration")

    if table_key not in config[step]:
        raise KeyError(f"Output table '{table_key}' not found in step '{step}'")

    table_config = config[step][table_key]['output_table']
    volume_root = get_volume_root(config)

    # Get subdirectory from config, raise error if not specified
    if 'subdirectory' not in table_config:
        raise KeyError(f"'subdirectory' not specified for output table '{table_key}' in step '{step}'")

    subdirectory = table_config['subdirectory']

    return DatabricksTableConfig(
        catalog=config['databricks']['destination']['catalog'],
        schema=config['databricks']['destination']['schema'],
        table=table_config['table'],
        volume_path=os.path.join(volume_root, subdirectory, table_config['filename']),
        sep=table_config.get('sep', '\t'),
        overwrite=table_config.get('overwrite', True),
    )