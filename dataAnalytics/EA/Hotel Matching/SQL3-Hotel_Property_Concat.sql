SELECT hotel_property_ID, 
REPLACE(
	REPLACE(
		REPLACE(
			REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												REPLACE(
													REPLACE(
														REPLACE(
															REPLACE(
																REPLACE(
																	REPLACE(
																		REPLACE(
																			REPLACE(
																				REPLACE(
																					REPLACE(
																						REPLACE(
																							LOWER(
																								GROUP_CONCAT(
																									concat(
																										CASE ISNULL(NAME) WHEN 1 THEN ' ' ELSE name END, ' ', 
																										CASE ISNULL(address_line_1) WHEN 1 THEN ' ' ELSE address_line_1 END, ' ', 
																										CASE ISNULL(address_line_2) WHEN 1 THEN ' ' ELSE address_line_2 END, ' ', 
																										CASE ISNULL(city_name) WHEN 1 THEN ' ' ELSE city_name END, ' ', 
																										CASE ISNULL(zip) WHEN 1 THEN ' ' ELSE zip END, ' ', 
																										CASE ISNULL(country) WHEN 1 THEN ' ' ELSE country END, ' ',
																										CASE ISNULL(state) WHEN 1 THEN ' ' ELSE state END) 																
																									SEPARATOR ' ')
																								),',',''
																							),'\n',' '
																						),'\t',' '
																					),'s\n',' '
																				),'s\\n',' '
																			),'.',''
																		),'\\', ' '
																	),']',''
																),'[',''
															),'?',''
														),'/', ' '
													),'|', ' '
												),')', ' '
											),'(', ' '
										),'the',''
									),'hotel',''
								),'null', ''
							),'#',''
						),'"', ''
					),'-',' '
				),'&',' '
			),'  ',' '
		),'   ',' '
	) as name
FROM cleaned
where hotel_property_ID is not null
GROUP BY hotel_property_ID 