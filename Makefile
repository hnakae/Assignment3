m1_tester:
	rm -rf CS451
	python3 m1_tester.py

m2_tester:
	rm -rf CS451
	python3 m2_tester_part1.py
	python3 m2_tester_part2.py

m3_tester:
	rm -rf CS451
	python3 m3_tester_part_1.py
# 	python3 m3_tester_part_2.py

exam_tester_m2:
	rm -rf CS451
	python3 exam_tester_m2_part1.py
	python3 exam_tester_m2_part2.py

clean: 
	rm -rf CS451
	rm -rf lstore/__pycache__

.PHONY: run m1_tester m2_tester exam_tester_m2 clean