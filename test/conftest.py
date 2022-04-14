from datetime import datetime
import platform
import os

def pytest_cmdline_preparse(config, args):

    datetime_now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_file = f'test/reports/{datetime_now_str}-quality-report.html'

    
    # config.option.htmlpath = html_file
    # config.option.self_contained_html = True
    args.extend(['--html', html_file, '--self-contained-html'])