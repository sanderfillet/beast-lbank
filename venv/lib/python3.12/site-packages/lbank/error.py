class Error(Exception):
    pass


class ClientError(Error):
    def __init__(
            self, status_code, error_code, error_message,
            error_data=None):
        self.status_code = status_code
        self.error_code = error_code
        self.error_message = error_message
        self.error_data = error_data

    def __str__(self):
        return '''
        status_code: %s, error_code: %s,
        error_message: %s, error_data: %s''' % (
            self.status_code, self.error_code, self.error_message,
            self.error_data
        )


class ServerError(Error):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message

    def __str__(self):
        return '''
        status_code: %s, message: %s''' % (
            self.status_code, self.message
        )


class CommonError(Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return '''
        message: %s''' % (
            self.message
        )
