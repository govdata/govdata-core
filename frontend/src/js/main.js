require(["find","jquery","jquery.address"], 
	function(find) {
		$.address.init(function(e) {
			console.log("my address init");
		}).externalChange(function(e) {
			console.log(e);
			var state = $.address.jsonhash();
			var params = $.address.parameters();
			console.log(state);
			console.log(params);
			if(e.path === "/show") { // show
				console.log("SHOW");
				show.load(params,state);
			} else { // find
				console.log("FIND");
				find.load(params,state);
			}
		});
		$(function() {
			console.log("HI");
			// find.onLoad();
		});
});

