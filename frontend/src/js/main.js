require(["find","jquery","jquery.address"], 
	function(find) {
		$.address.init(function(e) {

		}).externalChange(function(e) {
			var state = $.address.jsonhash();
			state = state || {};
			var params = $.address.parameters();
			if (e.path === "show") { // show
				console.log("SHOW");
				show.load(params,state);
			} else  { // find
				$.address.path('find');
				find.load(params,state);				
			}

		});

});

