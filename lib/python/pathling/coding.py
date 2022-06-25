class Coding:
    """
    A Coding represents a code in a code system.
    See: https://hl7.org/fhir/R4/datatypes.html#Coding
    """

    def __init__(self, system: str, code: str, version: str = None, display: str = None,
                 user_selected: bool = None):
        """
        :param system: a URI that identifies the code system
        :param code: the code
        :param version: a URI that identifies the version of the code system
        :param display: the display text for the Coding
        :param user_selected: an indicator of whether the Coding was chosen directly by the user
        """
        self.system = system
        self.code = code
        self.version = version
        self.display = display
        self.user_selected = user_selected
