@echo off
echo Launching multiple brokers, providers and consumers

start python .\dist_context_broker.py -cfg_file .\testing\broker_config_file_broker1
timeout /t 5 /nobreak >nul
start python .\dist_context_broker.py -cfg_file .\testing\broker_config_file_broker2
timeout /t 5 /nobreak >nul
start python .\dist_context_broker.py -cfg_file .\testing\broker_config_file_broker3
timeout /t 5 /nobreak >nul
start python .\dist_context_broker.py -cfg_file .\testing\broker_config_file_broker4
timeout /t 5 /nobreak >nul
start python .\dist_context_broker.py -cfg_file .\testing\broker_config_file_broker5
timeout /t 5 /nobreak >nul
start python .\testing\test_context_provider_kaiserslautern.py
timeout /t 5 /nobreak >nul
start python .\testing\test_context_provider_porto.py
timeout /t 5 /nobreak >nul
start python .\testing\test_context_consumer_porto.py
timeout /t 5 /nobreak >nul
