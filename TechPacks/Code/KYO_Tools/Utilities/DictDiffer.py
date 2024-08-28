class DictDiffer(object):
    """
    Class for calculating the difference between two dictionaries.

    """

    def __init__(self, current_dict, past_dict):
        '''Initialised with the two dictionary objects to be compared'''
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        '''Returns:
                Dictionary of added items'''
        return self.set_past - self.intersect

    def removed(self):
        '''Returns:
                Dictionary of removed items'''
        return self.set_current - self.intersect

    def changed(self):
        '''Returns:
                Dictionary of changed items'''
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])

    def unchanged(self):
        '''Returns:
                Dictionary of unchanged items'''
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])

    def common(self):
        '''Returns:
                Dictionary of common items'''
        return self.intersect