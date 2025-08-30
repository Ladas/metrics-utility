from metrics_utility.automation_controller_billing.report.report_ccsp import ReportCCSP
from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance
from metrics_utility.exceptions import NotSupportedFactory


class Factory:  # ReportFactory
    def __init__(self, dataframes, extra_params):
        self.dataframes = dataframes
        self.extra_params = extra_params

    def create(self):
        report_type = self.extra_params['report_type']
        print(f'DEBUG: ReportFactory creating report type: {report_type}')

        if report_type == 'CCSP':
            print('DEBUG: Creating ReportCCSP instance...')
            return ReportCCSP(self.dataframes, self.extra_params)

        if report_type == 'CCSPv2':
            print('DEBUG: Creating ReportCCSPv2 instance...')
            return ReportCCSPv2(self.dataframes, self.extra_params)

        if report_type == 'RENEWAL_GUIDANCE':
            print('DEBUG: Creating ReportRenewalGuidance instance...')
            return ReportRenewalGuidance(self.dataframes, self.extra_params)

        raise NotSupportedFactory(f'Factory for {report_type} not supported')
