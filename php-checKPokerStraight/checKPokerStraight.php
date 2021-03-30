<?php

	$resp = isStraight([14,5,4,2,3]); //true
	var_dump($resp);
	$resp = isStraight([9,10,11,12,13]);  //true
	var_dump($resp);
	$resp = isStraight([2,7,8,5,10,9,11]); //true
	var_dump($resp);
	$resp = isStraight([7,8,12,13,14]);  //false
	var_dump($resp);
	$resp = isStraight([2,3,4,5,6]);  //true
	var_dump($resp);
	$resp = isStraight([14,5,4,2,3]); //true
	var_dump($resp);
	$resp = isStraight([7,7,12,11,3,4,14]); //false
	var_dump($resp);
	$resp = isStraight([7,3,2]); //false
	var_dump($resp);
	$resp = isStraight([6,2,5,4,3]); //true
	var_dump($resp);
	$resp = isStraight([14,2,13,12,3]); //true
	var_dump($resp);
	$resp = isStraight([10,11,12,13,14]); //true
	var_dump($resp);
	$resp = isStraight([3,11,12,13,14]); //false
	var_dump($resp);
	$resp = isStraight([13,14,2,3,6]); //false
	var_dump($resp);
	$resp = isStraight([13,14,2,3,4]); //true
	var_dump($resp);
	$resp = isStraight([12,13,14,2,3,7,7]); //true
	var_dump($resp);
	$resp = isStraight([5,14,2,7,4,11,3]); //true
	var_dump($resp);
	$resp = isStraight([12,13,14,2,3]); //true
	var_dump($resp);
	$resp = isStraight([11,13,14,2,3]); //false
	var_dump($resp);

	/**
	 * [isStraight Function that verifies if a set of cards is a poker straight or not]
	 * @author Oscar García Chávez. <oscar.garcia@wom.cl>
	 * @version 1.0	Version Base
	 * @param  [array]  $cards [Matrix of int representing a list of poker cards]
	 * @return boolean [true = is a straight, false = is not a straight]
	 */
	function isStraight($cards){

		// Cards values ​​accepted
		$cardsAllowed = [2,3,4,5,6,7,8,9,10,11,12,13,14];
		// Cards count
		$cardsCount = count($cards);
		// Count of cards validated
		$cardCheckedCount = count(array_intersect($cards, $cardsAllowed));

		// If the number of cards is not between 5 and 7, or the number of validated cards is less than the original it is not a straight
		if($cardsCount < 5 || $cardsCount > 7 || $cardsCount != $cardCheckedCount) {
			return false;
		}else{
			for($i = 2; $i <= 14; $i++) {
			    $iAux = $i;
			    // If the value is not in the card array, continue with the next value
			    if (!in_array($iAux, $cards)){
			    	continue;
			    }
			    // Populate a possible array of values ​​to compare with the card array
			    $straightAux = [$iAux, auxCardValue($iAux), auxCardValue($iAux), auxCardValue($iAux), auxCardValue($iAux)];
			    // Compare with cards array, if all match return true and stop execution.
			    if (count(array_intersect($straightAux, $cards)) == count($straightAux)) {
			    	return true;
			    }
			}

			// If not straight found, return false
			return false;
		}
	}

	/**
	 * [auxCardValue Function that transform and return receiving value for populate the array of possible straights]
	 * @author Oscar García Chávez. <oscar.garcia@wom.cl>
	 * @version 1.0	Version Base
	 * @param  [int]  &$i [int value of a poker card]
	 * @return int [possible card value]
	 */
	function auxCardValue(&$i) {
		if ($i == 14){
			$i = 2;
		}
		else{
			$i++;
		}

		return $i;
	}
 ?>