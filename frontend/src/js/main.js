require(["lib/jquery-ui-1.8.6.custom.min","underscore","extensions","find"], 
	function(ui,_,ext,find) {
		$(function() {
			console.log("init");
			find.onLoad();
		});
});

