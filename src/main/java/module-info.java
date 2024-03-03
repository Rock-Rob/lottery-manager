module org.rg.game.lottery {

	requires com.fasterxml.jackson.databind;
	requires com.formdev.flatlaf;
	requires java.desktop;
	requires java.logging;
    requires jdk.unsupported;
	requires org.apache.poi.ooxml;
	requires org.apache.poi.poi;
	requires org.apache.xmlbeans;
	requires org.burningwave;
	requires org.burningwave.json;
	requires org.burningwave.reflection;
	requires org.jsoup;
	requires firebase.admin;
    requires google.cloud.core;
    requires google.cloud.firestore;
    requires com.google.auth;
    requires com.google.api.apicommon;
    requires com.google.auth.oauth2;
	requires gax;

	opens org.rg.game.lottery.application to com.fasterxml.jackson.databind;
	exports org.rg.game.lottery.application;

}
