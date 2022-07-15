class AbstractMover(object):
    
    # overridables
    def task(self, task_id):
        """
        Returns task by ID
        """
        return None

    def current_tasks(self):
        """
        returns list of current tasks - active and queued
        """
        return []
        
    def recent_tasks(self):
        """
        returns list of recently ended tasks
        """
        return []

    def task_history(self):
        """
        returns list of tasks from long term history
        """
        return []

    def stop(self):
        pass
        
    def quarantined(self):
        """
        Returns tuple: [file descriptors], error
        """
        return files or [], None
        
    def add_files(self, file_list):
        """
        adds files to be processd. file_list is the list of FileDesc objects
        """
        pass
