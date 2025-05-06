

class SureTaxHeaderError(Exception):
    errors = {
        '1100': 'Failure - General Failure',
        '1101': 'Failure - There is a format error in the web request - Input string was not in a correct format.',
        '1110': 'Failure - Data Month Required',
        '1120': 'Failure - Data Year Required',
        '1121': 'Failure - Invalid Data Year/Month - Must be a published Data Year/Month',
        '1130': 'Failure - Client Number Required',
        '1131': 'Failure - Invalid Client Number',
        '1141': 'Failure - Invalid Business Unit',
        '1150': 'Failure - Validation Key Required',
        '1151': 'Failure - Invalid Validation Key',
        '1160': 'Failure - Total Revenue Required',
        '1171': 'Failure - Invalid Client Tracking Code',
        '1190': 'Failure - Response Group Code Required',
        '1191': 'Failure - Invalid Response Group Code',
        '1200': 'Failure - Response Type Required',
        '1201': 'Failure - Invalid Response Type',
        '1210': 'Failure - Return File Code Required',
        '1211': 'Failure - Invalid Return File Code',
        '1220': 'Failure - Item List Required',
        '1510': 'Failure - Transaction is more than 60 days old (generated from Cancel Request method)'
    }

    def __init__(self, error_code):
        self.error_code = error_code

        try:
            self.message = self.errors[self.error_code]
        except KeyError:
            self.message = 'Unknown Error %s' % self.error_code

    def __str__(self):
        return self.message


class SureTaxItemError(Exception):
    def __init__(self, error_code, item_details=[]):
        self.item_details = item_details

    def __str__(self):
        return ', '.join(set([x['Message'] for x in self.item_details]))