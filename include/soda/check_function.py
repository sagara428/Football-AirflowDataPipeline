'''
Module: check_function

This module contains a function to run data quality checks using Soda.
'''

from soda.scan import Scan

def check(scan_name, checks_subpath=None,
          data_source='international_football', project_root='include'):
    '''
    Run data quality checks using Soda.

    Args:
        scan_name (str): The name of the scan.
        checks_subpath (str, optional): The subpath for checks. Default is None.
        data_source (str, optional): Data source name. Default is 'international_football'.
        project_root (str, optional): The root directory of the project. Default is 'include'.

    Returns:
        int: The result code of the Soda scan. 0 indicates success.

    Raises:
        ValueError: If the Soda scan fails.

    Example usage:
    check('check_load', checks_subpath='sources')
    '''
    print('Running Soda Scan ...')
    config_file = f'{project_root}/soda/configuration.yml'
    checks_path = f'{project_root}/soda/checks'

    if checks_subpath:
        checks_path += f'/{checks_subpath}'

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())

    if result != 0:
        raise ValueError('Soda Scan failed')

    return result
