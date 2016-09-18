#!/usr/bin/env groovy

package genddl

/**
 * Holds Time Periods
 */
class TimePeriods {

  def years() {
    [
      "2009",
      "2010",
      "2011",
      "2012",
      "2013",
      "2014",
      "2015",
      "2016"
        ];
  }

  def validYears() {
    def validYears = years();
    def years = years();
    years.each {
      def fromYear = it;
      years.each {
        def toYear = it;
        if (fromYear.toInteger() <= toYear.toInteger()) {
          validYears << "${fromYear}-${toYear}";
        }
      }
    }
    def fys = []
    validYears.each {
      fys << "FY${it.replace(/-/,'-FY')}"
    }
    fys as String[];
  }

  // for quick testing
  static main(args) {
    if (args[0] in new TimePeriods().validYears()) {
      println("yes");
    }
    else {
      println("no");
    }
  }

}
