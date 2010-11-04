goog.provide('gov.history');

gov.history = new goog.History();
gov.history.setEnabled(true);

gov.history.navCallback = function(e) {
    console.log(e.token);
}

goog.events.listen( gov.history,
                    goog.history.EventType.NAVIGATE,
                    gov.history.navCallback);


