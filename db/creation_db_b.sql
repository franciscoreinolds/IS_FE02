-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema fe02_hl7_db_b
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema fe02_hl7_db_b
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `fe02_hl7_db_b` DEFAULT CHARACTER SET utf8 ;
USE `fe02_hl7_db_b` ;

-- -----------------------------------------------------
-- Table `fe02_hl7_db_b`.`worklist`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fe02_hl7_db_b`.`worklist` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `patient_id` INT NOT NULL,
  `process` INT NOT NULL,
  `request_id` INT NOT NULL,
  `episode_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
