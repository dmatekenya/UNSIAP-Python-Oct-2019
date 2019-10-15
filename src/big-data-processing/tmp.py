class myCallback(tf.keras.callbacks.Callback):
    """
     A utility class which allows us to stop
    training model when a specified accuracy is reached.
    """
    def on_epoch_end(self, epoch, logs={}):
        """
        This function is called at the end of each epoch to check if the desired
        accuracy metric is reached. If its reached, the training is stopped
        :param epoch:
        :param logs:
        :return:
        """
        if logs.get('acc') > 0.6:
            print("\nReached 60% accuracy so cancelling training!")
            self.model.stop_training = True
