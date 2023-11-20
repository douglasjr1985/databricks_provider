import argparse
import logging
import json

from toolkit.resourcecontroller import Config

def configure_logging():
    """Configures the logging settings."""
    logging.basicConfig(filename='log_file.log', level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='Process modified Jobs.')
    parser.add_argument('--workspace_url', type=str, required=True, help='Workspace URL')
    parser.add_argument('--client_secret', type=str, required=True, help='Client Secret')
    parser.add_argument('--path_config', type=str, required=True, help='Path Config')
    return parser.parse_args()

def main():
    # Configure logging
    configure_logging()

    # Parse command-line arguments
    args = parse_arguments()

    # Log the initiation of job processing
    logging.info(f'Processing job: {args.path_config}')

    # Create a Config instance and execute the job
    config_instance = Config(args.workspace_url, args.client_secret, args.path_config)
    config_instance.execute()

if __name__ == '__main__':
    main()