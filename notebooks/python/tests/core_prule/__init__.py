import logging

logging.basicConfig(
    filename='tmp/test.log',
    filemode='w',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s: %(message)s',
)
